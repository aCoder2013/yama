(defproject jepsen-yama "0.1.0-SNAPSHOT"
  :description "Jepsen linearizability verification for Yama Raft KV"
  :license {:name "Apache-2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  :main jepsen.yama.core
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.1.19"]
                 [clj-http "3.12.3"]]
  :profiles {:dev {:resource-paths ["resources"]}})
