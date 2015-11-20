(ns onyx.plugin.bookkeeper
  (:require [clojure.core.async :refer [chan >! >!! <!! close! thread timeout alts!! go-loop sliding-buffer]]
            [onyx.state.log.bookkeeper :as obk]
            [onyx.compression.nippy :as nippy]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.peer.function :as function]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [defaults]]
            [onyx.log.zookeeper :as zk]
            [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info debug fatal]])
  (:import [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper BookKeeper$DigestType AsyncCallback$AddCallback]))

(defn start-commit-loop! [commit-ch log k]
  (go-loop []
           (when-let [content (<!! commit-ch)]
             (extensions/force-write-chunk log :chunk content k)
             (recur))))

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; read BookKeeper log plugin

(defn close-read-ledgers-resources
  [{:keys [bookkeeper/producer-ch bookkeeper/commit-ch bookkeeper/read-ch bookkeeper/shutdown-ch] :as event} lifecycle]
  (close! read-ch)
  (close! commit-ch)
  (close! shutdown-ch)
  (<!! producer-ch)
  {})

(defn set-starting-offset! [log task-map checkpoint-key start-id]
  (if (:checkpoint/force-reset? task-map)
    (extensions/force-write-chunk log :chunk {:largest (or start-id -1)
                                              :status :incomplete}
                                  checkpoint-key)
    (extensions/write-chunk log :chunk {:largest (or start-id -1)
                                        :status :incomplete}
                            checkpoint-key)))

(defn validate-within-supplied-bounds [start-id end-id checkpoint-id]
  (when checkpoint-id
    (when (and start-id (< checkpoint-id start-id))
      (throw (ex-info "Checkpointed transaction is less than :bookkeeper/log-start-id"
                      {:bookkeeper/ledger-start-id start-id
                       :bookkeeper/ledger-end-id end-id
                       :checkpointed-id checkpoint-id})))
    (when (and end-id (>= checkpoint-id end-id))
      (throw (ex-info "Checkpointed transaction is greater than :bookkeeper/log-start-id"
                      {:bookkeeper/ledger-start-id start-id
                       :bookkeeper/ledger-end-id end-id
                       :checkpointed-id checkpoint-id})))))

(defn check-completed [task-map checkpointed]
  (when (and (not (:checkpoint/key task-map))
             (= :complete (:status checkpointed)))
    (throw (Exception. "Restarted task, however it was already completed for this job.
                       This is currently unhandled."))))

(defn read-ledger-entries [ledger-handle last-acked read-ch]
  (info "Starting BooKeeper input ledger reader at:" (inc last-acked))
  (let [ledger-id (.getId ledger-handle)
        last-confirmed (.getLastAddConfirmed ledger-handle)]
    ;; Naive, synchronous non-chunked approach that reads everything
    (if-not (and (neg? last-confirmed)
                 (not= last-confirmed last-acked))
      (loop [entries (.readEntries ledger-handle (inc last-acked) last-confirmed)
             ledger-entry (.nextElement entries)] 
        (let [segment {:entry-id (.getEntryId ^LedgerEntry ledger-entry) 
                       :ledger-id ledger-id
                       :value (nippy/decompress (.getEntry ^LedgerEntry ledger-entry))}] 
          (>!! read-ch (t/input (java.util.UUID/randomUUID) segment))
          (if (.hasMoreElements entries)
            (recur entries 
                   (.nextElement entries))))))))

(defn inject-read-ledgers-resources
  [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/pipeline onyx.core/peer-opts] :as event} lifecycle]
  (when-not (= 1 (:onyx/max-peers task-map))
    (throw (ex-info "Read log tasks must set :onyx/max-peers 1" task-map)))
  (let [start-id (:bookkeeper/ledger-start-id task-map)
        max-id (:bookkeeper/ledger-end-id task-map)
        {:keys [read-ch shutdown-ch commit-ch]} pipeline
        checkpoint-key (or (:checkpoint/key task-map) task-id)
        _ (set-starting-offset! log task-map checkpoint-key start-id)
        checkpointed (extensions/read-chunk log :chunk checkpoint-key)
        _ (validate-within-supplied-bounds start-id max-id (:largest checkpointed))
        _ (check-completed task-map checkpointed)
        read-size (or (:bookkeeper/read-max-chunk-size task-map) 1000)
        batch-timeout (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout defaults))
        initial-backoff 1
        commit-loop-ch (start-commit-loop! commit-ch log checkpoint-key)
        ledgers-root-path (or (:bookkeeper/zookeeper-ledgers-root-path task-map)
                               (zk/ledgers-path (:onyx/id peer-opts)))
        zookeeper-addr (:bookkeeper/zookeeper-addr task-map)
        zookeeper-timeout 60000
        bookkeeper-throttle 30000
        client (obk/bookkeeper zookeeper-addr ledgers-root-path zookeeper-timeout bookkeeper-throttle)
        ledger-id (:bookkeeper/ledger-id task-map)
        password (or (:bookkeeper/password task-map) (throw (Exception. ":bookkeeper/password must be supplied")))
        ledger-handle (obk/open-ledger client ledger-id obk/digest-type password)
        producer-ch (thread
                      (try
                        (let [exit (loop [last-acked (:largest checkpointed)]
                                     ;if (first (alts!! [shutdown-ch] :default true))
                                     (read-ledger-entries ledger-handle last-acked read-ch)
                                     :finished)]
                          (if-not (= exit :shutdown)
                            (>!! read-ch (t/input (java.util.UUID/randomUUID) :done))))
                        (catch Exception e
                          (fatal e))))]
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
      (>!! read-ch (assoc msg :id (java.util.UUID/randomUUID))))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn read-ledgers [pipeline-data]
  (let [catalog-entry (:onyx.core/task-map pipeline-data)
        max-pending (or (:onyx/max-pending catalog-entry) (:onyx/max-pending defaults))
        batch-size (:onyx/batch-size catalog-entry)
        batch-timeout (or (:onyx/batch-timeout catalog-entry) (:onyx/batch-timeout defaults))
        read-ch (chan (or (:bookkeeper/read-buffer catalog-entry) 1000))
        commit-ch (chan (sliding-buffer 1))
        shutdown-ch (chan 1)
        top-id (atom -1)
        top-acked-id (atom -1)
        pending-indexes (atom #{})]
    (->BookKeeperLogInput (:onyx.core/log pipeline-data)
                          (:onyx.core/task-id pipeline-data)
                          max-pending batch-size batch-timeout
                          (atom {})
                          (atom false)
                          top-id
                          top-acked-id
                          pending-indexes
                          read-ch
                          commit-ch
                          shutdown-ch)))

(def read-ledgers-calls
  {:lifecycle/before-task-start inject-read-ledgers-resources
   :lifecycle/after-task-stop close-read-ledgers-resources})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; output plugins

#_(comment 
  (defn inject-write-tx-resources
    [{:keys [onyx.core/pipeline]} lifecycle]
    {:bookkeeper/conn (:conn pipeline)})

(defn inject-write-bulk-tx-resources
  [{:keys [onyx.core/pipeline]} lifecycle]
  {:bookkeeper/conn (:conn pipeline)})

(defrecord BookKeeperWriteDatoms [conn partition]
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ event]
    (let [messages (mapcat :leaves (:tree (:onyx.core/results event)))]
      @(d/transact conn
                   (map (fn [msg] (if (and partition (not (sequential? msg)))
                                    (assoc msg :db/id (d/tempid partition))
                                    msg))
                        (map :message messages)))
      {:onyx.core/written? true}))

  (seal-resource
    [_ _]
    {}))

(defn write-datoms [pipeline-data]
  (let [task-map (:onyx.core/task-map pipeline-data)
        conn (safe-connect task-map)
        partition (:bookkeeper/partition task-map)]
    (->BookKeeperWriteDatoms conn partition))))
