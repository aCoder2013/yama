(ns jepsen.yama.ports
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def default-ports
  {"n1" 19001
   "n2" 19002
   "n3" 19003})

(defn ports-file
  []
  (or (System/getenv "YAMA_JEPSEN_PORTS_FILE")
      "cluster-ports.edn"))

(defn load-ports
  "Load node->port map from cluster-ports.edn or fall back to defaults."
  []
  (let [f (io/file (ports-file))]
    (if (.exists f)
      (edn/read-string (slurp f))
      default-ports)))

(defn node->port
  [node]
  (let [ports (load-ports)
        k (name node)]
    (or (get ports k)
        (get ports node)
        (get default-ports k)
        19001)))

(defn servers-string
  []
  (let [ports (load-ports)]
    (->> (sort-by key ports)
         (map (fn [[node port]]
                (str "127.0.0.1:" port)))
         (str/join ";"))))

(defn base-url
  [node]
  (str "http://127.0.0.1:" (node->port node) "/yama/raft/api/v1"))
