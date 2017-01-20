(ns onyx.plugin.bookkeeper
  (:require [clojure.core.async :refer [chan >! >!! <!! close! offer! poll! thread timeout alts!! go-loop sliding-buffer]]
            [schema.core :as s]
            [onyx.schema :as onyx-schema]
            [onyx.types :as t]
            [onyx.static.default-vals :refer [default-vals arg-or-default]]
            [onyx.bookkeeper.utils :refer [bookkeeper open-ledger open-ledger-no-recovery]]
            [clojure.core.async.impl.protocols :refer [closed?]]
            [onyx.log.zookeeper :as log-zk]
            [onyx.log.curator :as zk]
            [onyx.extensions :as extensions]
            [onyx.protocol.task-state :refer :all]
            [onyx.plugin.protocols.plugin :as p]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [onyx.monitoring.measurements :refer [measure-latency]]
            [onyx.compression.nippy :refer [zookeeper-compress zookeeper-decompress]]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.static.util :refer [kw->fn]]
            [taoensso.timbre :refer [info warn error trace debug fatal]])
  (:import [org.apache.zookeeper KeeperException$BadVersionException
            KeeperException$ConnectionLossException]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper
            BKException$Code BKException$ZKException BookKeeper$DigestType
            AsyncCallback$AddCallback]))

(def BookKeeperInput
  {:bookkeeper/zookeeper-addr s/Str
   :bookkeeper/digest-type (s/enum :mac :crc32)
   :bookkeeper/deserializer-fn onyx-schema/NamespacedKeyword
   (s/optional-key :bookkeeper/ledger-start-id) onyx-schema/SPosInt
   (s/optional-key :bookkeeper/ledger-end-id) onyx-schema/SPosInt
   (s/optional-key :bookkeeper/no-recovery?) s/Bool
   (s/optional-key :bookkeeper/read-max-chunk-size) onyx-schema/PosInt
   (s/optional-key :bookkeeper/zookeeper-ledgers-root-path) s/Str
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

(def digest-type {:crc32 BookKeeper$DigestType/CRC32 
                  :mac BookKeeper$DigestType/MAC})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; read BookKeeper log plugin

(defn close-read-ledgers-resources
  [{:keys [bookkeeper/producer-ch bookkeeper/commit-ch bookkeeper/read-ch bookkeeper/retry-ch bookkeeper/shutdown-ch] :as event} lifecycle]
  (info "Closing read ledger resources:" (:onyx.core/task event))
  {})


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

(def read-chunk-size 100)

(defn default-value [task-map k v]
  (update task-map k (fn [curr] (or curr v))))

(deftype BookKeeperLogInput 
  [log-prefix                        ^:unsynchronized-mutable task-map 
   batch-timeout                                              deserializer-fn
   ^:unsynchronized-mutable client   ^:unsynchronized-mutable ledger-handle
   ^:unsynchronized-mutable digest   ^:unsynchronized-mutable entries 
   ^:unsynchronized-mutable offset   ^:unsynchronized-mutable drained]
  p/Plugin
  (start [this {:keys [onyx.core/log onyx.core/job-id onyx.core/task-id onyx.core/peer-opts] :as event}] 
    (let [task-map* (:onyx.core/task-map event)
          _ (when-not (= 1 (:onyx/max-peers (:onyx.core/task-map event)))
              (throw (ex-info "Read log tasks must set :onyx/max-peers 1" task-map)))
          _ (when-not (:bookkeeper/password-bytes task-map)
              (throw (Exception. ":bookkeeper/password-bytes must be supplied")))
          _ (info log-prefix "Inject read ledger resources:" task-id)
          defaulted-task-map (-> task-map*
                                 (default-value :bookkeeper/read-max-chunk-size 1000)
                                 (default-value :onyx/batch-timeout (:onyx/batch-timeout default-vals))
                                 (default-value :bookkeeper/zookeeper-ledgers-root-path (:onyx.bookkeeper/zk-ledgers-root-path default-vals))
                                 (default-value :bookkeeper/ledger-start-id 0)
                                 (default-value :bookkeeper/ledger-end-id Double/POSITIVE_INFINITY)
                                 (default-value :checkpoint/key (str job-id "#" task-id)))
          {:keys [bookkeeper/ledger-start-id bookkeeper/ledger-end-id bookkeeper/zookeeper-ledgers-root-path
                  bookkeeper/zookeeper-addr bookkeeper/no-recovery?]} defaulted-task-map
          ;_ (validate-within-supplied-bounds (dec ledger-start-id) ledger-end-id (:largest checkpointed))
          zookeeper-timeout 60000
          bookkeeper-throttle 30000
          digest* (digest-type (:bookkeeper/digest-type defaulted-task-map))]
      (set! client (bookkeeper zookeeper-addr zookeeper-ledgers-root-path zookeeper-timeout bookkeeper-throttle))
      (set! digest digest*)
      (set! task-map defaulted-task-map)
      this))

  (stop [this event] 
    this)

  i/Input
  (checkpoint [this]
    (inc offset))

  (recover! [this replica-version checkpoint]
    (set! drained false)
    (set! offset (or checkpoint (:bookkeeper/ledger-start-id task-map)))
    ;(seek-offset! log-prefix consumer kpartition task-map topic checkpoint)
    this)

  (synced? [this epoch]
    true)

  (checkpointed! [this epoch])

  (poll! [this _]
    (when-not ledger-handle
      (let [f (if (:bookkeeper/no-recovery? task-map)
                open-ledger-no-recovery
                open-ledger)] 
        (set! ledger-handle
              (f client (:bookkeeper/ledger-id task-map) digest 
                 (:bookkeeper/password-bytes task-map)))))
    (if (and entries (.hasMoreElements entries))
      (let [ledger-entry (.nextElement entries)
            entry-id (.getEntryId ^LedgerEntry ledger-entry)]
        (set! offset (inc entry-id))
        {:entry-id entry-id
         :ledger-id (.getId ledger-handle)
         :value (deserializer-fn (.getEntry ^LedgerEntry ledger-entry))})
      (let [chunk-size 1
            last-confirmed (.getLastAddConfirmed ledger-handle)
            ledger-end-id (min last-confirmed (:bookkeeper/ledger-end-id task-map))]
        (if (or drained
                (> offset ledger-end-id))
          (do (set! drained true)
              (set! entries nil))
          (set! entries (.readEntries ledger-handle 
                                      offset
                                      (min (+ chunk-size offset) 
                                           ledger-end-id
                                           last-confirmed))))
        nil)))

  (completed? [this]
    drained))

(defn read-ledgers [{:keys [onyx.core/task-map onyx.core/log onyx.core/task-id onyx.core/log-prefix] :as pipeline-data}]
  (let [batch-timeout (or (:onyx/batch-timeout task-map) (:onyx/batch-timeout default-vals))
        batch-size (:onyx/batch-size task-map)
        deserializer-fn (kw->fn (:bookkeeper/deserializer-fn task-map))]
    (validate-task-map! task-map BookKeeperInput)
    (->BookKeeperLogInput log-prefix
                          task-map
                          batch-timeout
                          deserializer-fn
                          nil
                          nil
                          nil
                          nil
                          nil
                          false)))

(defn read-handle-exception [event lifecycle lf-kw exception]
  (let [exception-type (type exception)] 
    (case exception-type 
      org.apache.zookeeper.KeeperException$ConnectionLossException
      :restart
      org.apache.bookkeeper.client.BKException$ZKException
      :restart
      :defer)))

(def read-ledgers-calls
  {:lifecycle/handle-exception read-handle-exception})

;;;;;;;;;;;;;
;;;;;;;;;;;;;
;; output plugins

; (defn inject-write-ledger-resources
;   [{:keys [onyx.core/pipeline]} lifecycle]
;   {:bookkeeper/client (:client pipeline)
;    :bookkeeper/ledger-handle (:ledger-handle pipeline)})

; (defn close-write-ledger-resources
;   [{:keys [bookkeeper/client bookkeeper/ledger-handle] :as event} lifecycle]
;   (.close client)
;   {})

; (defn write-handle-exception [event lifecycle lf-kw exception]
;   (let [exception-type (type exception)] 
;     (case exception-type 
;       org.apache.zookeeper.KeeperException$ConnectionLossException
;       :restart
;       org.apache.bookkeeper.client.BKException$ZKException
;       :restart
;       :defer)))

; (def write-ledger-calls
;   {:lifecycle/before-task-start inject-write-ledger-resources
;    :lifecycle/handle-exception write-handle-exception
;    :lifecycle/after-task-stop close-write-ledger-resources})

; (def HandleWriteCallback
;   (reify AsyncCallback$AddCallback
;     (addComplete [this rc lh entry-id ack]
;       (try 
;         (if (= rc (BKException$Code/OK))
;           ((:ack-fn ack))
;           ((:failed! ack) rc))
;         (catch Throwable t
;           (error t))))))

; (defrecord BookKeeperWriteLedger [client ledger-handle serializer-fn write-failed-code]
;   p-ext/Pipeline
;   (read-batch
;     [_ event]
;     (function/read-batch event))

;   (write-batch
;     [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger] :as event}]
;     (when @write-failed-code
;       (throw (ex-info "Write to BookKeeper ledger failed." {:ledger-id (.getId ledger-handle)
;                                                             :code @write-failed-code})))
;     (doall
;       (map (fn [[result ack]]
;              (run! (fn [_]
;                      (inc-count! ack))
;                    (:leaves result))
;              (let [ack-fn (fn [] 
;                             (when (dec-count! ack)
;                               (when-let [site (peer-site peer-replica-view (:completion-id ack))]
;                                 (extensions/internal-ack-segment messenger site ack))))
;                    failed-reset-fn (fn [code] (reset! write-failed-code code))
;                    callback-data {:ack-fn ack-fn :failed! failed-reset-fn}] 
;                (run! (fn [leaf]
;                        (.asyncAddEntry ^LedgerHandle ledger-handle 
;                                        ^bytes (serializer-fn (:message leaf))
;                                        HandleWriteCallback
;                                        callback-data))
;                      (:leaves result))))
;            (map list (:tree results) (:acks results))))
;     {:bookkeeper/written? true})

;   (seal-resource
;     [_ _]
;     (.close ledger-handle)
;     {}))

; (defn add-ledger-data! [{:keys [conn] :as log} path ledger-id]
;   (let [bytes (zookeeper-compress [ledger-id])]
;     (when-not (zk/create-all conn path :persistent? true :data bytes)
;       (while (try 
;                (let [current (zk/data conn path)
;                      version (:version (:stat current))
;                      data (zookeeper-decompress (:data current))
;                      new-data (conj data ledger-id)]
;                  (zk/set-data conn path (zookeeper-compress new-data) version)
;                  false)
;                (catch org.apache.zookeeper.KeeperException$BadVersionException t
;                  (info (format "Couldn't add ledger under: %s. Retrying." path))
;                  true))))))

; (defn bookkeeper-write-ledger-ids-path [onyx-id & path-args]
;   (str (log-zk/catalog-path onyx-id) "/" (clojure.string/join "/" path-args) "/ledgers"))

; (defn read-ledgers-data [{:keys [conn] :as log} onyx-id job-id task-id]
;   (let [node (bookkeeper-write-ledger-ids-path onyx-id job-id task-id)]
;     (zookeeper-decompress (:data (zk/data conn node)))))

; (defn write-ledger [{:keys [onyx.core/task-map onyx.core/log onyx.core/peer-opts onyx.core/task-id onyx.core/job-id] :as pipeline-data}]
;   (validate-task-map! task-map BookKeeperOutput)
;   (let [onyx-id (:onyx/tenancy-id peer-opts)
;         ledgers-root-path (or (:bookkeeper/zookeeper-ledgers-root-path task-map)
;                               (:onyx.bookkeeper/zk-ledgers-root-path default-vals))
;         zookeeper-addr (:bookkeeper/zookeeper-addr task-map)
;         ;; FIXME, parameterize these in the task-map
;         zookeeper-timeout 60000
;         bookkeeper-throttle 30000
;         client (bookkeeper zookeeper-addr ledgers-root-path zookeeper-timeout bookkeeper-throttle)
;         serializer-fn (kw->fn (:bookkeeper/serializer-fn task-map))
;         digest (digest-type (:bookkeeper/digest-type task-map))
;         password (or (:bookkeeper/password-bytes task-map) 
;                      (throw (Exception. ":bookkeeper/password-bytes must be supplied")))
;         ensemble-size (:bookkeeper/ensemble-size task-map)
;         quorum-size (:bookkeeper/quorum-size task-map)
;         ledger-handle (create-ledger client ensemble-size quorum-size digest password)
;         write-failed-code (atom false)
;         ledger-id (.getId ledger-handle)]
;     (info "BookKeeper write-ledger: created new ledger:" ledger-id)
;     (add-ledger-data! log (bookkeeper-write-ledger-ids-path onyx-id job-id task-id) ledger-id)
;     (->BookKeeperWriteLedger client ledger-handle serializer-fn write-failed-code)))

; ;;;;;;;;;;;;;;;;;;
; ;; Lifecycle only for use in triggers etc

; (defn inject-new-ledger
;   [{:keys [onyx.core/task-map onyx.core/log onyx.core/peer-opts onyx.core/task-id onyx.core/job-id] :as event} lifecycle]
;   (let [onyx-id (:onyx/tenancy-id peer-opts)
;         ledgers-root-path (or (:bookkeeper/zookeeper-ledgers-root-path lifecycle)
;                               (:onyx.bookkeeper/zk-ledgers-root-path default-vals))
;         zookeeper-addr (:bookkeeper/zookeeper-addr lifecycle)
;         ;; FIXME, parameterize these in the lifecycle
;         zookeeper-timeout 60000
;         bookkeeper-throttle 30000
;         client (bookkeeper zookeeper-addr ledgers-root-path zookeeper-timeout bookkeeper-throttle)
;         serializer-fn (kw->fn (:bookkeeper/serializer-fn lifecycle))
;         digest (digest-type (:bookkeeper/digest-type lifecycle))
;         password (or (:bookkeeper/password-bytes lifecycle) 
;                      (throw (Exception. ":bookkeeper/password-bytes must be supplied")))
;         ensemble-size (:bookkeeper/ensemble-size lifecycle)
;         quorum-size (:bookkeeper/quorum-size lifecycle)
;         ledger-handle (create-ledger client ensemble-size quorum-size digest password)
;         ledger-id (.getId ledger-handle)
;         ledger-data-path (bookkeeper-write-ledger-ids-path onyx-id job-id task-id)]
;     (info "BookKeeper write-ledger lifecycle: created new ledger:" ledger-id)
;     (add-ledger-data! log ledger-data-path ledger-id)
;     {:bookkeeper/client client
;      :bookkeeper/serializer-fn serializer-fn
;      :bookkeeper/ledger-data-path ledger-data-path
;      :bookkeeper/ledger-handle ledger-handle}))

; (defn close-new-ledger-resources
;   [{:keys [bookkeeper/client bookkeeper/ledger-handle] :as event} lifecycle]
;   (.close client)
;   {})

; (def new-ledger-calls
;   {:lifecycle/before-task-start inject-new-ledger
;    :lifecycle/after-task-stop close-new-ledger-resources})
