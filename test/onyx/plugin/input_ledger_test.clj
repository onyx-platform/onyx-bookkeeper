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

(def id "ONYXID")

(def zk-addr 
  "127.0.0.1:2188")

(def env-config
  {:zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2188
   :onyx.bookkeeper/server? true
   :onyx.bookkeeper/local-quorum? true
   :onyx/id id})

(def peer-config
  {:zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"
   :onyx.messaging/backpressure-strategy :high-restart-latency
   :onyx/id id})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def ledgers-root-path 
  (zk/ledgers-path id))

(def client 
  (obk/bookkeeper zk-addr
                  60000
                  30000))

(def ledger-handle 
  (obk/new-ledger client env-config))

(def n-entries 20)

(mapv (fn [v]
        (.addEntry ledger-handle (nippy/compress {:value v})))
      (range n-entries))

(.close ledger-handle)

(def batch-size 20)

(def out-chan (chan 1000))

(def workflow
  [[:read-ledgers :persist]])

(def catalog
  [{:onyx/name :read-ledgers
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
    :onyx/doc "Writes segments to a core.async channel"}])

(defn inject-persist-ch [event lifecycle]
  {:core.async/chan out-chan})

(def persist-calls
  {:lifecycle/before-task-start inject-persist-ch})

(def lifecycles
  [{:lifecycle/task :read-ledgers
    :lifecycle/calls :onyx.plugin.bookkeeper/read-ledgers-calls}
   {:lifecycle/task :persist
    :lifecycle/calls ::persist-calls}
   {:lifecycle/task :persist
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 3 peer-group))

(def job-id
  (:job-id (onyx.api/submit-job
             peer-config
             {:catalog catalog :workflow workflow :lifecycles lifecycles
              :task-scheduler :onyx.task-scheduler/balanced})))

(def results (take-segments! out-chan))

(onyx.api/await-job-completion peer-config job-id)

(fact (mapv :value (butlast results))
      =>
      (map (fn [v]
             {:value v})
           (range n-entries)))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)


