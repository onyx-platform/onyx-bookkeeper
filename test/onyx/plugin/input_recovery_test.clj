(ns onyx.plugin.input-recovery-test
  (:require [clojure.core.async :refer [chan >!! <!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.bookkeeper]
            [onyx.log.zookeeper :as zk]
            [onyx.bookkeeper.bookkeeper :as bkserver]
            [com.stuartsierra.component :as component]
            [onyx.api]
            [onyx.bookkeeper.utils :refer [bookkeeper new-ledger]]
            [onyx.compression.nippy :as nippy]
            [taoensso.timbre :refer [info error debug fatal]]
            [onyx.test-helper :refer [with-test-env]]
            [clojure.test :refer :all])
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper BookKeeper$DigestType AsyncCallback$AddCallback]))

(def out-chan (atom nil))

(defn inject-persist-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def persist-calls
  {:lifecycle/before-task-start inject-persist-ch})

(def batch-num (atom 0))

(def start-time (atom nil))

(def test-state (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (reset! test-state extent-state))

(def read-ledgers-crash
  {:lifecycle/before-batch (fn [event lifecycle]
                             ; give the peer a bit of time to write
                             ;; the chunks out and ack the batches
                             (when (and @start-time (> (System/currentTimeMillis) (+ @start-time 3000)))
                               (reset! start-time nil)
                               (throw (ex-info "Restartable" {:restartable? true}))))
   :lifecycle/handle-exception (constantly :restart)})

(deftest input-plugin
  (dotimes [i 1] 
    (let [_ (reset! out-chan (chan 1000))
          id (java.util.UUID/randomUUID)
          zk-addr "127.0.0.1:2188"
          env-config {:zookeeper/address zk-addr
                      :zookeeper/server? true
                      :zookeeper.server/port 2188
                      :onyx.bookkeeper/server? false
                      :onyx.bookkeeper/local-quorum? false
                      :onyx/tenancy-id id}
          peer-config {:zookeeper/address zk-addr
                       :onyx.peer/coordinator-barrier-period-ms 1
                       :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                       :onyx.peer/storage :s3
                       :onyx.peer/storage.s3.bucket "onyx-s3-testing"
                       :onyx.peer/storage.s3.region "us-west-2"
                       :onyx.messaging/impl :aeron
                       :onyx.messaging/peer-port 40200
                       :onyx.messaging/bind-addr "localhost"
                       :onyx.messaging/backpressure-strategy :high-restart-latency
                       :onyx/tenancy-id id}
          batch-size 3]
      (with-test-env [env [3 env-config peer-config]]
        (let [_ (reset! out-chan (chan 50000))
              log (:log (:env env))
              bk-config (assoc env-config 
                               :onyx.bookkeeper/server? true 
                               :onyx.bookkeeper/delete-server-data? true
                               :onyx.bookkeeper/local-quorum? true)
              multi-bookie-server (component/start (bkserver/multi-bookie-server bk-config log))] 
          (try 
           (let [ledgers-root-path "/ledgers"
                 client (bookkeeper zk-addr ledgers-root-path 60000 30000)
                 ledger-handle (new-ledger client env-config)
                 workflow [[:read-ledgers :persist]]
                 start (rand-int 100)
                 end (+ start (rand-int 50))
                 n-entries (inc end)
                 catalog [{:onyx/name :read-ledgers
                           :onyx/plugin :onyx.plugin.bookkeeper/read-ledgers
                           :onyx/type :input
                           :onyx/medium :bookkeeper
                           :bookkeeper/zookeeper-addr zk-addr
                           :bookkeeper/zookeeper-ledgers-root-path ledgers-root-path
                           :bookkeeper/ledger-id (.getId ledger-handle)
                           :bookkeeper/digest-type :mac
                           :bookkeeper/deserializer-fn :onyx.compression.nippy/zookeeper-decompress
                           :bookkeeper/ledger-start-id start
                           :bookkeeper/ledger-end-id end
                           :bookkeeper/read-max-chunk-size (inc (rand-int 100))
                           :bookkeeper/no-recovery? true
                           :bookkeeper/password-bytes (.getBytes "INSECUREDEFAULTPASSWORD")
                           :onyx/max-peers 1
                           :onyx/batch-size batch-size
                           :onyx/doc "Reads from a BookKeeper ledger"}

                          {:onyx/name :persist
                           :onyx/plugin :onyx.plugin.core-async/output
                           :onyx/type :output
                           :onyx/medium :core.async
                           :onyx/batch-size 20
                           :onyx/max-peers 1
                           :onyx/doc "Writes segments to a core.async channel"}]
                 lifecycles [{:lifecycle/task :read-ledgers
                              :lifecycle/calls :onyx.plugin.bookkeeper/read-ledgers-calls}
                             {:lifecycle/task :read-ledgers
                              :lifecycle/calls ::read-ledgers-crash}
                             {:lifecycle/task :persist
                              :lifecycle/calls ::persist-calls}
                             {:lifecycle/task :persist
                              :lifecycle/calls :onyx.plugin.core-async/writer-calls}]

                 windows [{:window/id :collect-segments
                           :window/task :read-ledgers
                           :window/type :global
                           :window/aggregation :onyx.windowing.aggregation/conj}]
                 triggers [{:trigger/window-id :collect-segments
                            :trigger/fire-all-extents? true
                            :trigger/id :collect-trigger
                            :trigger/on :onyx.triggers/segment
                            :trigger/threshold [1 :elements]
                            :trigger/sync ::update-atom!}]
                 entries (mapv (fn [v]
                                 {:value v :random (rand-int 10)})
                               (range n-entries))
                 ;; write start
                 _ (doseq [v (take 10 entries)]
                     (.addEntry ledger-handle (nippy/zookeeper-compress v)))
                 _ (reset! start-time (System/currentTimeMillis))
                 job-id (:job-id (onyx.api/submit-job
                                  peer-config
                                  {:catalog catalog :workflow workflow :lifecycles lifecycles
                                   :windows windows :triggers triggers
                                   :task-scheduler :onyx.task-scheduler/balanced}))
                 _ (future
                    (Thread/sleep 10000)
                    ;; Write rest
                    (doseq [v (drop 10 entries)]
                      (.addEntry ledger-handle (nippy/zookeeper-compress v)))
                    (.close ledger-handle))
                 _ (onyx.test-helper/feedback-exception! peer-config job-id)
                 results (take-segments! @out-chan 50)]
             ;; When live reading we can close the ledger after we've been reading
             (is (= (take (inc (- end start)) (drop start entries)) 
                    (map :value @test-state))))
           (finally
            (component/stop multi-bookie-server)))))))) 
