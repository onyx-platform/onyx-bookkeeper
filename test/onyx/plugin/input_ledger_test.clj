(ns onyx.plugin.input-ledger-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.bookkeeper]
            [onyx.log.zookeeper :as zk]
            [onyx.api]
            [onyx.state.log.bookkeeper :as obk]
            [onyx.compression.nippy :as nippy]
            [onyx.test-helper :refer [with-test-env]]
            [midje.sweet :refer :all])
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper BookKeeper$DigestType AsyncCallback$AddCallback]))

(def out-chan (atom nil))

(defn inject-persist-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def persist-calls
  {:lifecycle/before-task-start inject-persist-ch})

(let [_ (reset! out-chan (chan 1000))
      id "ONYXID"
      zk-addr "127.0.0.1:2188"
      env-config {:zookeeper/address zk-addr
                  :zookeeper/server? true
                  :zookeeper.server/port 2188
                  :onyx.bookkeeper/server? true
                  :onyx.bookkeeper/local-quorum? true
                  :onyx/id id}
      peer-config {:zookeeper/address zk-addr
                   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                   :onyx.messaging/impl :aeron
                   :onyx.messaging/peer-port 40200
                   :onyx.messaging/bind-addr "localhost"
                   :onyx.messaging/backpressure-strategy :high-restart-latency
                   :onyx/id id}
      ledgers-root-path (zk/ledgers-path id)
      client (obk/bookkeeper zk-addr ledgers-root-path 60000 30000)
      ledger-handle (obk/new-ledger client env-config)
      
      ;; add data to ledger
      n-entries 20
      _ (mapv (fn [v]
                (.addEntry ledger-handle (nippy/compress {:value v})))
              (range n-entries))
      _ (.close ledger-handle)

      batch-size 20
      workflow [[:read-ledgers :persist]]
      catalog [{:onyx/name :read-ledgers
                :onyx/plugin :onyx.plugin.bookkeeper/read-ledgers
                :onyx/type :input
                :onyx/medium :bookkeeper
                :bookkeeper/zookeeper-addr zk-addr
                :bookkeeper/zookeeper-ledgers-root-path ledgers-root-path
                :bookkeeper/ledger-id (.getId ledger-handle)
                ;:checkpoint/key "global-checkpoint-key"
                ;:checkpoint/force-reset? true
                :bookkeeper/password (.getBytes "INSECUREDEFAULTPASSWORD")
                :onyx/max-peers 1
                ;:bookkeeper/log-end-tx 1002
                :onyx/batch-size batch-size
                :onyx/doc "Reads a sequence of datoms from the d/tx-range API"}

               {:onyx/name :persist
                :onyx/plugin :onyx.plugin.core-async/output
                :onyx/type :output
                :onyx/medium :core.async
                :onyx/batch-size 20
                :onyx/max-peers 1
                :onyx/doc "Writes segments to a core.async channel"}]
      lifecycles [{:lifecycle/task :read-ledgers
                   :lifecycle/calls :onyx.plugin.bookkeeper/read-ledgers-calls}
                  {:lifecycle/task :persist
                   :lifecycle/calls ::persist-calls}
                  {:lifecycle/task :persist
                   :lifecycle/calls :onyx.plugin.core-async/writer-calls}]]

  (with-test-env [env [3 env-config peer-config]]
    (println "started env " env)
    #_(let [job-id (:job-id (onyx.api/submit-job
                            peer-config
                            {:catalog catalog :workflow workflow :lifecycles lifecycles
                             :task-scheduler :onyx.task-scheduler/balanced}))
          results [] ;(take-segments! @out-chan)

          ;_ (onyx.api/await-job-completion peer-config job-id)
          ]


      (fact (mapv :value (butlast results))
            =>
            (map (fn [v]
                   {:value v})
                 (range n-entries)))))) 
