(ns jepsen.yama.db
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.db :as db]))

(defn local-db
  "No-op DB for --local mode: cluster is started by scripts/local-cluster.sh."
  []
  (reify db/DB
    (setup! [_ _test _node]
      (info "local mode: assuming Yama Raft cluster already running"))
    (teardown! [_ _test _node]
      (info "local mode: leaving cluster running"))))

(defn db-for-test
  [local?]
  (if local?
    (local-db)
    (throw (ex-info "Remote SSH deployment not yet automated; use --local with scripts/local-cluster.sh"
                    {:local? local?}))))
