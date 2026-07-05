(ns jepsen.yama.client
  (:require [clojure.tools.logging :refer [debug warn]]
            [clj-http.client :as http]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [jepsen.yama.ports :as ports]
            [slingshot.slingshot :refer [try+]]))

(def ^:private timeout-ms 8000)
(def ^:private write-confirm-attempts 50)
(def ^:private write-confirm-sleep-ms 100)

(defn- key->str [k] (str "jepsen-" k))
(defn- val->str [v] (str v))

(defn- encode-param [s]
  (java.net.URLEncoder/encode s "UTF-8"))

(defn- get-url [node k]
  (str (ports/base-url node) "/get?key=" (encode-param (key->str k))))

(defn- put-url [node k v]
  (str (ports/base-url node) "/put?key=" (encode-param (key->str k))
       "&value=" (encode-param (val->str v))))

(defn- parse-value [body]
  (when body
    (try (Long/parseLong body)
         (catch Exception _ body))))

(defn- http-get [node k]
  (let [resp (http/get (get-url node k)
                       {:socket-timeout timeout-ms
                        :conn-timeout   timeout-ms
                        :throw-exceptions false})]
    (case (:status resp)
      200 {:ok true :value (:body resp)}
      503 {:ok false :error :unavailable}
      {:ok false :error (str "HTTP " (:status resp) ": " (:body resp))})))

(defn- http-put [node k v]
  (let [resp (http/get (put-url node k v)
                       {:socket-timeout timeout-ms
                        :conn-timeout   timeout-ms
                        :throw-exceptions false})]
    (case (:status resp)
      200 (if (= "ok" (:body resp))
            {:ok true}
            {:ok false :error :bad-body})
      {:ok false :error (str "HTTP " (:status resp))})))

(defn- confirm-write [node k v]
  (loop [i 0]
    (let [res (http-get node k)]
      (cond
        (and (:ok res) (= (val->str v) (:value res)))
        true

        (>= i write-confirm-attempts)
        false

        :else
        (do (Thread/sleep write-confirm-sleep-ms)
            (recur (inc i)))))))

(defn- parse-kv
  [value]
  (if (instance? clojure.lang.MapEntry value)
    [(key value) (val value)]
    value))

(defrecord YamaClient [node]
  client/Client
  (open! [this _test node] (assoc this :node node))
  (setup! [_ _test])
  (teardown! [_ _test])

  (invoke! [_ _test op]
    (let [[k v] (parse-kv (:value op))]
      (try+
        (case (:f op)
          :read
          (let [res (http-get node k)]
            (if (:ok res)
              (assoc op :type :ok
                         :value (independent/tuple k (parse-value (:value res))))
              (assoc op :type :fail :error (:error res))))

          :write
          (let [put-res (http-put node k v)]
            (cond
              (not (:ok put-res))
              (assoc op :type :info :error (:error put-res))

              (confirm-write node k v)
              (assoc op :type :ok)

              :else
              (assoc op :type :info :error :write-not-visible)))

          :cas
          (let [[old new] v
                cur (http-get node k)]
            (cond
              (not (:ok cur))
              (assoc op :type :fail :error (:error cur))

              (not= (parse-value (:value cur)) old)
              (assoc op :type :fail :error :stale)

              (not (:ok (http-put node k new)))
              (assoc op :type :info :error :cas-write-failed)

              (confirm-write node k new)
              (assoc op :type :ok)

              :else
              (assoc op :type :info :error :cas-not-visible))))

        (catch java.net.SocketTimeoutException _
          (assoc op :type (if (= :read (:f op)) :fail :info) :error :timeout))

        (catch Exception e
          (warn e "client error on" node op)
          (assoc op :type :info :error (.getMessage e))))))

  (close! [_ _test]))

(defn client
  []
  (->YamaClient nil))
