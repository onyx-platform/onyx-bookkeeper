(ns onyx.plugin.bookkeeper
  (:require [clojure.core.async :refer [chan >! >!! <!! close! thread timeout alts!! go-loop sliding-buffer]]
            [schema.core :as s]
            [onyx.schema :as onyx-schema]
            [onyx.state.log.bookkeeper :as obk]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.log.zookeeper :as log-zk]
            [onyx.log.curator :as zk]
            [onyx.extensions :as extensions]
            [onyx.monitoring.measurements :refer [measure-latency]]
            [onyx.compression.nippy :refer [zookeeper-compress zookeeper-decompress]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.types :refer [dec-count! inc-count!]]
            [taoensso.timbre :refer [info error debug fatal]])
  (:import [org.apache.zookeeper KeeperException$BadVersionException KeeperException$ConnectionLossException]
           [org.apache.bookkeeper.client 
            LedgerHandle LedgerEntry BookKeeper BKException$Code BKException$ZKException BookKeeper$DigestType AsyncCallback$AddCallback]))

(def BookKeeperInput
  {:bookkeeper/zookeeper-addr s/Str
   :bookkeeper/digest-type (s/enum :mac :crc32)
   :bookkeeper/deserializer-fn onyx-schema/NamespacedKeyword
   (s/optional-key :bookkeeper/ledger-start-id) onyx-schema/SPosInt
   (s/optional-key :bookkeeper/ledger-end-id) onyx-schema/SPosInt
   (s/optional-key :bookkeeper/no-recovery?) s/Bool
   (s/optional-key :bookkeeper/no-recovery-empty-read-back-off) onyx-schema/SPosInt
   (s/optional-key :bookkeeper/read-max-chunk-size) onyx-schema/PosInt
   (s/optional-key :bookkeeper/zookeeper-ledgers-root-path) s/Str
   (s/optional-key :checkpoint/force-reset?) s/Bool
   ;; need password
   s/Any s/Any})

(def BookKeeperOutput
  {:bookkeeper/zookeeper-addr s/Str
   :bookkeeper/digest-type (s/enum :mac :crc32)
   :bookkeeper/serializer-fn onyx-schema/NamespacedKeyword
   :bookkeeper/ensemble-size onyx-schema/SPosInt
   :bookkeeper/quorum-size onyx-schema/SPosInt
   s/Any s/Any})

(defn validate-task-map! [task-map schema]
  (try (s/validate schema task-map)
       (catch Throwable t
         (error t "Failed schema check on task map." task-map schema)
         (throw t))))

(defn start-commit-loop! [commit-ch log k]
  (go-loop []
           (when-let [content (<!! commit-ch)]
             (extensions/force-write-chunk log :chunk content k)
             (recur))))

(def digest-type {:crc32 BookKeeper$DigestType/CRC32 
                  :mac BookKeeper$DigestType/MAC})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; read BookKeeper log plugin

(defn close-read-ledgers-resources
  [{:keys [bookkeeper/producer-ch bookkeeper/commit-ch bookkeeper/read-ch bookkeeper/shutdown-ch] :as event} lifecycle]
  ;; Before we waited for producer ch to return. We should probably still do so by checking the shutdown-ch
  (close! shutdown-ch)
  (close! read-ch)
  (close! commit-ch)
  {})

(defn set-starting-offset! [log task-map checkpoint-key start]
  (if (:checkpoint/force-reset? task-map)
    (extensions/force-write-chunk log :chunk {:largest start
                                              :status :incomplete}
                                  checkpoint-key)
    (extensions/write-chunk log :chunk {:largest start
                                        :status :incomplete}
                            checkpoint-key)))

(defn validate-within-supplied-bounds [start-id end-id checkpoint-id]
  (when checkpoint-id
    (when (and start-id (< checkpoint-id start-id))
      (throw (ex-info "Checkpointed transaction is less than :bookkeeper/ledger-start-id"
                      {:bookkeeper/ledger-start-id start-id
                       :bookkeeper/ledger-end-id end-id
                       :checkpointed-id checkpoint-id})))
    (when (and end-id (>= checkpoint-id end-id))
      (throw (ex-info "Checkpointed transaction is greater than :bookkeeper/ledger-start-id"
                      {:bookkeeper/ledger-start-id start-id
                       :bookkeeper/ledger-end-id end-id
                       :checkpointed-id checkpoint-id})))))

(defn check-completed [task-map checkpointed]
  (when (and (not (:checkpoint/key task-map))
             (= :complete (:status checkpointed)))
    (throw (Exception. "Restarted task, however it was already completed for this job.
                       This is currently unhandled."))))

(defn read-ledger-chunk! [ledger-handle deserializer-fn read-ch start end]
  (loop [entries (.readEntries ledger-handle start end)
         ledger-entry (.nextElement entries)] 
    (let [segment {:entry-id (.getEntryId ^LedgerEntry ledger-entry) 
                   :ledger-id (.getId ledger-handle)
                   :value (deserializer-fn (.getEntry ^LedgerEntry ledger-entry))}] 
      (>!! read-ch (t/input (random-uuid) segment))
      (if (.hasMoreElements entries)
        (recur entries 
               (.nextElement entries))))))

(def read-chunk-size 100)

(defn read-ledger-entries! 
  [client {:keys [bookkeeper/ledger-id bookkeeper/password-bytes
                  bookkeeper/ledger-end-id bookkeeper/no-recovery-empty-read-back-off] :as task-map}
   digest read-ch deserializer-fn start]
  (let [ledger-handle (obk/open-ledger client ledger-id digest password-bytes)]
    (try
      (let [last-confirmed (.getLastAddConfirmed ledger-handle)
            bounded-end (min ledger-end-id last-confirmed)
            chunks (partition-all 2 1 (range (dec start) bounded-end read-chunk-size))
            _ (info "Starting final read: " start ledger-end-id last-confirmed bounded-end (vec chunks))]
        (run! (fn [[s e]]
                (read-ledger-chunk! ledger-handle deserializer-fn read-ch (inc s) (or e bounded-end)))
              chunks))
      (finally (.close ledger-handle)))))

(defn no-recovery-read-ledger-entries! 
  [client {:keys [bookkeeper/ledger-id bookkeeper/password-bytes 
                  bookkeeper/ledger-end-id bookkeeper/no-recovery-empty-read-back-off] :as task-map} 
   digest read-ch deserializer-fn start]
  (let [ledger-handle (obk/open-ledger-no-recovery client ledger-id digest password-bytes)
        last-confirmed (.getLastAddConfirmed ledger-handle)
        bounded-end (min ledger-end-id last-confirmed)
        chunks (partition-all 2 1 (range (dec start) bounded-end read-chunk-size))]
    (try 
      (run! (fn [[s e]]
              (read-ledger-chunk! ledger-handle deserializer-fn read-ch (inc s) (or e bounded-end)))
            chunks)
      (finally (.close ledger-handle)))

    (if (.isClosed client ledger-id)
      (read-ledger-entries! client task-map digest read-ch deserializer-fn (inc bounded-end))
      (do
        (Thread/sleep no-recovery-empty-read-back-off)
        (recur client task-map digest read-ch deserializer-fn (inc bounded-end))))))

(defn default-value [task-map k v]
  (update task-map k (fn [curr] (or curr v))))

(defn inject-read-ledgers-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline onyx.core/peer-opts] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "Read log tasks must set :onyx/max-peers 1" task-map)))
  (when-not (:bookkeeper/password-bytes task-map)
    (throw (Exception. ":bookkeeper/password-bytes must be supplied")))

  (let [{:keys [bookkeeper/ledger-start-id bookkeeper/ledger-end-id bookkeeper/zookeeper-ledgers-root-path
                bookkeeper/zookeeper-addr bookkeeper/deserializer-fn bookkeeper/no-recovery?] :as defaulted-task-map} 
        (-> task-map 
            (default-value :bookkeeper/read-max-chunk-size 1000)
            (default-value :onyx/batch-timeout (:onyx/batch-timeout defaults))
            (default-value :bookkeeper/zookeeper-ledgers-root-path (log-zk/ledgers-path (:onyx/id peer-opts)))
            (default-value :bookkeeper/ledger-start-id 0)
            (default-value :bookkeeper/ledger-end-id Double/POSITIVE_INFINITY)
            (default-value :bookkeeper/no-recovery-empty-read-back-off 500)
            (default-value :checkpoint/key task-id))
        {:keys [read-ch shutdown-ch commit-ch]} pipeline
        ;; decrement because we are going to store this as a checkpoint and then inc after recover
        checkpoint-key (:checkpoint/key defaulted-task-map)
        _ (set-starting-offset! log task-map checkpoint-key (dec ledger-start-id))
        checkpointed (extensions/read-chunk log :chunk checkpoint-key)
        _ (validate-within-supplied-bounds (dec ledger-start-id) ledger-end-id (:largest checkpointed))
        _ (check-completed task-map checkpointed)
        commit-loop-ch (start-commit-loop! commit-ch log checkpoint-key)
        zookeeper-timeout 60000
        bookkeeper-throttle 30000
        client (obk/bookkeeper zookeeper-addr zookeeper-ledgers-root-path zookeeper-timeout bookkeeper-throttle)
        deserializer-fn (kw->fn deserializer-fn)
        digest (digest-type (:bookkeeper/digest-type task-map))
        read-fn (if no-recovery?
                  no-recovery-read-ledger-entries!
                  read-ledger-entries!)
        producer-ch (thread
                      (try
                        (let [exit (loop [last-acked (inc (:largest checkpointed))]
                                     (read-fn client defaulted-task-map digest read-ch deserializer-fn last-acked)
                                     :finished)]
                          (if (= exit :finished)
                            (>!! read-ch (t/input (random-uuid) :done))))
                        (catch Exception e
                          (>!! read-ch (t/input (random-uuid) e))
                          (fatal e "BookKeeper plugin: error reading."))))]
    {:bookkeeper/read-ch read-ch
     :bookkeeper/shutdown-ch shutdown-ch
     :bookkeeper/commit-ch commit-ch
     :bookkeeper/producer-ch producer-ch
     :bookkeeper/drained? (:drained pipeline)
     :bookkeeper/pending-messages (:pending-messages pipeline)}))

(defn highest-acked-index [starting-index top-index pending-indexes]
  (loop [max-pending starting-index]
    (if (or (pending-indexes (inc max-pending))
            (= top-index max-pending))
      max-pending
      (recur (inc max-pending)))))

(defn all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defrecord BookKeeperLogInput
  [log task-id max-pending batch-size batch-timeout pending-messages drained?
   top-index top-acked-index pending-indexes read-ch commit-ch shutdown-ch]
  p-ext/Pipeline
  (write-batch
    [this event]
    (function/write-batch event))

  (read-batch
    [_ event]
    (let [pending (count @pending-messages)
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (if (zero? max-segments) 
                      (<!! timeout-ch)
                      (->> (range max-segments)
                           (keep (fn [_] (first (alts!! [read-ch timeout-ch] :priority true))))))]
      (doseq [m batch]
        (let [message (:message m)]
          (when (instance? java.lang.Throwable message)
            (throw message))

          (when-not (= message :done)
            (swap! top-index max (:entry-id message))
            (swap! pending-indexes conj (:entry-id message))))
        (swap! pending-messages assoc (:id m) m))
      (when (and (all-done? (vals @pending-messages))
                 (all-done? batch)
                 (or (not (empty? @pending-messages))
                     (not (empty? batch)))
                 (zero? (count (.buf read-ch))))
        (when-not (:checkpoint/key (:onyx.core/task-map event))
          (>!! commit-ch {:status :complete}))
        (reset! drained? true))
      {:onyx.core/batch batch}))

  p-ext/PipelineInput

  (ack-segment [_ _ segment-id]
    (let [entry-id (:entry-id (:message (@pending-messages segment-id)))]
      (swap! pending-indexes disj entry-id)
      ;; if this transaction is now the lowest unacked tx, then we can update the checkpoint
      (let [new-top-acked (highest-acked-index @top-acked-index @top-index @pending-indexes)]
        (>!! commit-ch {:largest new-top-acked :status :incomplete})
        (reset! top-acked-index new-top-acked))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ event segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (>!! read-ch (assoc msg :id (random-uuid))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn read-ledgers [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id] :as pipeline-data}]
  (let [max-pending (or (:onyx/max-pending task-map) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size task-map)
        batch-timeout (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:bookkeeper/read-buffer task-map) 1000))
        commit-ch (chan (sliding-buffer 1))
        shutdown-ch (chan 1)
        top-id (atom -1)
        top-acked-id (atom -1)
        pending-indexes (atom #{})]
    (validate-task-map! task-map BookKeeperInput)
    (->BookKeeperLogInput log
                          task-id
                          max-pending batch-size batch-timeout
                          (atom {})
                          (atom false)
                          top-id
                          top-acked-id
                          pending-indexes
                          read-ch
                          commit-ch
                          shutdown-ch)))

(defn read-handle-exception [event lifecycle lf-kw exception]
  (let [exception-type (type exception)] 
    (case exception-type 
      org.apache.zookeeper.KeeperException$ConnectionLossException
      :restart
      org.apache.bookkeeper.client.BKException$ZKException
      :restart
      :defer)))

(def read-ledgers-calls
  {:lifecycle/before-task-start inject-read-ledgers-resources
   :lifecycle/handle-exception read-handle-exception
   :lifecycle/after-task-stop close-read-ledgers-resources})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; output plugins

(defn inject-write-ledger-resources
  [{:keys [onyx.core/pipeline]} lifecycle]
  {:bookkeeper/client (:client pipeline)
   :bookkeeper/ledger-handle (:ledger-handle pipeline)})

(defn close-write-ledger-resources
  [{:keys [bookkeeper/client bookkeeper/ledger-handle] :as event} lifecycle]
  (.close client)
  {})

(defn write-handle-exception [event lifecycle lf-kw exception]
  (let [exception-type (type exception)] 
    (case exception-type 
      org.apache.zookeeper.KeeperException$ConnectionLossException
      :restart
      org.apache.bookkeeper.client.BKException$ZKException
      :restart
      :defer)))

(def write-ledger-calls
  {:lifecycle/before-task-start inject-write-ledger-resources
   :lifecycle/handle-exception write-handle-exception
   :lifecycle/after-task-stop close-write-ledger-resources})

(def HandleWriteCallback
  (reify AsyncCallback$AddCallback
    (addComplete [this rc lh entry-id ack]
      (if (= rc (BKException$Code/OK))
        ((:ack-fn ack))
        ((:failed! ack) rc)))))

(defrecord BookKeeperWriteLedger [client ledger-handle serializer-fn write-failed-code]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger] :as event}]
    (when @write-failed-code
      (throw (ex-info "Write to BookKeeper ledger failed." {:ledger-id (.getId ledger-handle)
                                                            :code @write-failed-code})))
    (doall
      (map (fn [[result ack]]
             (run! (fn [_]
                     (inc-count! ack))
                   (:leaves result))
             (let [ack-fn (fn [] 
                            (when (dec-count! ack)
                              (when-let [site (peer-site peer-replica-view (:completion-id ack))]
                                (extensions/internal-ack-segment messenger site ack))))
                   failed-reset-fn (fn [code] (reset! write-failed-code code))
                   callback-data {:ack-fn ack-fn :failed! failed-reset-fn}] 
               (run! (fn [leaf]
                       (.asyncAddEntry ^LedgerHandle ledger-handle 
                                       ^bytes (serializer-fn (:message leaf))
                                       HandleWriteCallback
                                       callback-data))
                     (:leaves result))))
           (map list (:tree results) (:acks results))))
    {:onyx.core/written? true})

  (seal-resource
    [_ _]
    (.close ledger-handle)
    {}))

(defn add-ledger-data! [{:keys [conn] :as log} path ledger-id]
  (let [bytes (zookeeper-compress [ledger-id])]
    (when-not (zk/create-all conn path :persistent? true :data bytes)
      (while (try 
               (let [current (zk/data conn path)
                     version (:version (:stat current))
                     data (zookeeper-decompress (:data current))
                     new-data (conj data ledger-id)]
                 (zk/set-data conn path (zookeeper-compress new-data) version)
                 false)
               (catch org.apache.zookeeper.KeeperException$BadVersionException t
                 (info (format "Couldn't add ledger under: %s. Retrying." path))
                 true))))))

(defn bookkeeper-write-ledger-ids-path [onyx-id & path-args]
  (str (log-zk/catalog-path onyx-id) "/" (clojure.string/join "/" path-args) "/ledgers"))

(defn read-ledgers-data [{:keys [conn] :as log} onyx-id job-id task-id]
  (let [node (bookkeeper-write-ledger-ids-path onyx-id job-id task-id)]
    (zookeeper-decompress (:data (zk/data conn node)))))

(defn write-ledger [{:keys [onyx.core/task-map onyx.core/log onyx.core/peer-opts onyx.core/task-id onyx.core/job-id] :as pipeline-data}]
  (validate-task-map! task-map BookKeeperOutput)
  (let [onyx-id (:onyx/id peer-opts)
        ledgers-root-path (or (:bookkeeper/zookeeper-ledgers-root-path task-map)
                              (log-zk/ledgers-path onyx-id))
        zookeeper-addr (:bookkeeper/zookeeper-addr task-map)
        ;; FIXME, parameterize these in the task-map
        zookeeper-timeout 60000
        bookkeeper-throttle 30000
        client (obk/bookkeeper zookeeper-addr ledgers-root-path zookeeper-timeout bookkeeper-throttle)
        serializer-fn (kw->fn (:bookkeeper/serializer-fn task-map))
        digest (digest-type (:bookkeeper/digest-type task-map))
        password (or (:bookkeeper/password-bytes task-map) 
                     (throw (Exception. ":bookkeeper/password-bytes must be supplied")))
        ensemble-size (:bookkeeper/ensemble-size task-map)
        quorum-size (:bookkeeper/quorum-size task-map)
        ledger-handle (obk/create-ledger client ensemble-size quorum-size digest password)
        write-failed-code (atom false)
        ledger-id (.getId ledger-handle)]
    (info "BookKeeper write-ledger: created new ledger:" ledger-id)
    (add-ledger-data! log (bookkeeper-write-ledger-ids-path onyx-id job-id task-id) ledger-id)
    (->BookKeeperWriteLedger client ledger-handle serializer-fn write-failed-code)))

;;;;;;;;;;;;;;;;;;
;; Lifecycle only for use in triggers etc

(defn inject-new-ledger
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/peer-opts onyx.core/task-id onyx.core/job-id] :as event} lifecycle]
  (let [onyx-id (:onyx/id peer-opts)
        ledgers-root-path (or (:bookkeeper/zookeeper-ledgers-root-path lifecycle)
                              (log-zk/ledgers-path onyx-id))
        zookeeper-addr (:bookkeeper/zookeeper-addr lifecycle)
        ;; FIXME, parameterize these in the lifecycle
        zookeeper-timeout 60000
        bookkeeper-throttle 30000
        client (obk/bookkeeper zookeeper-addr ledgers-root-path zookeeper-timeout bookkeeper-throttle)
        serializer-fn (kw->fn (:bookkeeper/serializer-fn lifecycle))
        digest (digest-type (:bookkeeper/digest-type lifecycle))
        password (or (:bookkeeper/password-bytes lifecycle) 
                     (throw (Exception. ":bookkeeper/password-bytes must be supplied")))
        ensemble-size (:bookkeeper/ensemble-size lifecycle)
        quorum-size (:bookkeeper/quorum-size lifecycle)
        ledger-handle (obk/create-ledger client ensemble-size quorum-size digest password)
        ledger-id (.getId ledger-handle)
        ledger-data-path (bookkeeper-write-ledger-ids-path onyx-id job-id task-id)]
    (info "BookKeeper write-ledger lifecycle: created new ledger:" ledger-id)
    (add-ledger-data! log ledger-data-path ledger-id)
    {:bookkeeper/client client
     :bookkeeper/serializer-fn serializer-fn
     :bookkeeper/ledger-data-path ledger-data-path
     :bookkeeper/ledger-handle ledger-handle}))

(defn close-new-ledger-resources
  [{:keys [bookkeeper/client bookkeeper/ledger-handle] :as event} lifecycle]
  (.close client)
  {})

(def new-ledger-calls
  {:lifecycle/before-task-start inject-new-ledger
   :lifecycle/after-task-stop close-new-ledger-resources})
