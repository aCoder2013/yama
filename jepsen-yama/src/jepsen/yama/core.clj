(ns jepsen.yama.core
  (:require [clojure.tools.logging :refer :all]
            [jepsen.cli :as cli]
            [jepsen.core :as jepsen]
            [jepsen.tests :as tests]
            [jepsen.tests.linearizable-register :as lin-reg]
            [jepsen.os :as os]
            [jepsen.yama.client :as yama-client]
            [jepsen.yama.db :as yama-db]))

(defn yama-test
  [opts]
  (let [local? (:local opts false)
        nodes (:nodes opts)
        workload (lin-reg/test {:nodes nodes
                                :per-key-limit (:ops-per-key opts 64)})]
    (-> (merge tests/noop-test
               (select-keys opts [:nodes :concurrency :time-limit :test-count
                                   :leave-db-running? :logging-json?])
               workload
               {:name      (str "yama-raft-kv" (when local? "-local"))
                :os        os/noop
                :db        (yama-db/db-for-test local?)
                :client    (yama-client/client)
                :ssh       (if local?
                             {:dummy? true}
                             {:username "root"
                              :password "root"
                              :strict-host-key-checking false})})
        (dissoc :local :ops-per-key))))

(defn -main
  [& args]
  (cli/run!
    (merge (cli/single-test-cmd
             {:test-fn yama-test
              :opt-spec [[nil "--local" "Run against a pre-started local cluster (dummy SSH)"
                          :id :local :default false]
                         ["-k" "--ops-per-key NUM" "Max operations per key"
                          :id :ops-per-key
                          :parse-fn #(Long/parseLong %)
                          :default 64]]})
           (cli/serve-cmd))
    args))

(defn smoke-test
  [opts]
  (let [result (jepsen/run! (yama-test opts))]
    (info "Jepsen result:" (pr-str (:valid? (:results result))))
    result))
