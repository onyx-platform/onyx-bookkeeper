(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [com.stuartsierra.component :as component]
            [onyx.compression.nippy :as nippy]
            [onyx.log.zookeeper :as zk]
            [onyx.bookkeeper.bookkeeper :as bkserver]
            [onyx.bookkeeper.utils :refer [bookkeeper open-ledger open-ledger-no-recovery create-ledger]]
            [onyx.plugin.bookkeeper]
            [onyx.api]
            [onyx.test-helper :refer [with-test-env]]
            [taoensso.timbre :as log :refer [fatal info]]
            [clojure.test :refer :all])
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper BKException$Code
            BookKeeper$DigestType AsyncCallback$AddCallback]))

(def in-chan (atom nil))
(def in-buffer (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer in-buffer
   :core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(defn serialize-segment [segment]
  (.getBytes (pr-str segment)))

(deftest output-plugin-test  
  (let [id (java.util.UUID/randomUUID)
        zk-addr "127.0.0.1:2188"
        env-config {:zookeeper/address zk-addr
                    :zookeeper/server? true
                    :zookeeper.server/port 2188
                    :onyx.bookkeeper/server? true
                    :onyx.bookkeeper/local-quorum? true
                    :onyx.bookkeeper/ledger-ensemble-size 3
                    :onyx.bookkeeper/ledger-quorum-size 3
                    :onyx.bookkeeper/ledger-id-written-back-off 50
                    :onyx.bookkeeper/ledger-password "INSECUREDEFAULTPASSWORD"
                    :onyx.bookkeeper/client-throttle 30000
                    :onyx/tenancy-id id}

        peer-config {:zookeeper/address zk-addr
                     :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                     :onyx.peer/coordinator-barrier-period-ms 2000
                     :onyx.messaging/impl :aeron
                     :onyx.messaging/peer-port 40199
                     :onyx.messaging/bind-addr "localhost"
                     :onyx/tenancy-id id}

        workflow [[:in :identity]
                  [:identity :write-messages]]

        password (.getBytes "bkpass")
        catalog [{:onyx/name :in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/max-peers 1
                  :onyx/batch-size 100
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :identity
                  :onyx/fn :clojure.core/identity
                  :onyx/type :function
                  :onyx/batch-size 100}

                 {:onyx/name :write-messages
                  :onyx/plugin :onyx.plugin.bookkeeper/write-ledger
                  :onyx/type :output
                  :onyx/n-peers 2
                  :onyx/medium :bookkeeper
                  :bookkeeper/serializer-fn :onyx.compression.nippy/zookeeper-compress
                  :bookkeeper/password-bytes password
                  :bookkeeper/ensemble-size 3
                  :bookkeeper/quorum-size 3
                  :bookkeeper/zookeeper-addr zk-addr
                  :bookkeeper/digest-type :mac
                  :onyx/batch-size 50
                  :onyx/doc "Writes messages to a BookKeeper ledger"}]

        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls :onyx.plugin.output-test/in-calls}
                    {:lifecycle/task :in
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :write-messages
                     :lifecycle/calls :onyx.plugin.bookkeeper/write-ledger-calls}]
        _ (reset! in-buffer {})
        n-messages 10000
        _ (reset! in-chan (chan n-messages))]

    (with-test-env [env [4 env-config peer-config]]
      (let [log (:log (:env env))
            bk-config (assoc env-config 
                             :onyx.bookkeeper/server? true 
                             :onyx.bookkeeper/delete-server-data? true
                             :onyx.bookkeeper/local-quorum? true)
            multi-bookie-server (component/start (bkserver/multi-bookie-server bk-config log))] 
        (try 
         (let [input-values (mapv (fn [v] {:a v})
                                  (range n-messages))
               job (onyx.api/submit-job
                    peer-config
                    {:catalog catalog 
                     :workflow workflow 
                     :lifecycles lifecycles
                     :task-scheduler :onyx.task-scheduler/balanced})
               _ (doseq [v input-values]
                   (>!! @in-chan v))
               _ (close! @in-chan)
               _ (onyx.test-helper/feedback-exception! peer-config (:job-id job))
               job-id (:job-id job)
               output (onyx.plugin.bookkeeper/read-ledgers-data (:log (:env env)) id job-id :write-messages)]
           (is (= input-values
                  (sort-by :a
                           (mapcat (fn [ledger-id]
                                     (let [digest BookKeeper$DigestType/MAC
                                           ledgers-root-path "/ledgers"
                                           zookeeper-timeout 60000
                                           bookkeeper-throttle 30000
                                           client (bookkeeper zk-addr ledgers-root-path zookeeper-timeout bookkeeper-throttle)
                                           ledger-handle (open-ledger client ledger-id digest password)
                                           start 0
                                           end (.getLastAddConfirmed ledger-handle)
                                           entries (.readEntries ledger-handle start end)]
                                       (loop [values []
                                              ledger-entry (.nextElement entries)] 
                                         (let [entry (nippy/zookeeper-decompress (.getEntry ^LedgerEntry ledger-entry))
                                               new-entries (conj values entry)]
                                           (if (.hasMoreElements entries)
                                             (recur new-entries (.nextElement entries))
                                             new-entries)))))
                                   output)))))
         (finally (component/stop multi-bookie-server)))))))
