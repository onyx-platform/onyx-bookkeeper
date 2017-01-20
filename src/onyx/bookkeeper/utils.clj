(ns onyx.bookkeeper.utils
  (:require [onyx.static.default-vals :refer [default-vals arg-or-default]])
  (:import [org.apache.zookeeper KeeperException$BadVersionException
            KeeperException$ConnectionLossException]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.bookkeeper.client LedgerHandle LedgerEntry BookKeeper
            BKException$Code BKException$ZKException BookKeeper$DigestType
            AsyncCallback$AddCallback]))

(defn open-ledger ^org.apache.bookkeeper.client.LedgerHandle [^BookKeeper client id digest-type password]
  (.openLedger client id digest-type password))

(defn open-ledger-no-recovery ^org.apache.bookkeeper.client.LedgerHandle [^BookKeeper client id digest-type password]
  (.openLedgerNoRecovery client id digest-type password))

(defn create-ledger ^org.apache.bookkeeper.client.LedgerHandle [^BookKeeper client ensemble-size quorum-size digest-type password]
  (.createLedger client ensemble-size quorum-size digest-type password))

(defn close-handle [^LedgerHandle ledger-handle]
  (.close ledger-handle))

(defn ^org.apache.bookkeeper.client.BookKeeper bookkeeper
  ([opts]
   (bookkeeper (:zookeeper/address opts)
               (arg-or-default :onyx.bookkeeper/zk-ledgers-root-path opts)
               (arg-or-default :onyx.bookkeeper/client-timeout opts)
               (arg-or-default :onyx.bookkeeper/client-throttle opts)))
  ([zk-addr zk-root-path timeout throttle]
   (try
    (let [conf (doto (ClientConfiguration.)
                 (.setZkServers zk-addr)
                 (.setZkTimeout timeout)
                 (.setThrottleValue throttle)
                 (.setZkLedgersRootPath zk-root-path))]
      (BookKeeper. conf))
    (catch org.apache.zookeeper.KeeperException$NoNodeException nne
      (throw (ex-info "Error locating BookKeeper cluster via ledger path. Check that BookKeeper has been started via start-env by setting `:onyx.bookkeeper/server? true` in env-config, or is setup at the correct path." 
                      {:zookeeper-addr zk-addr
                       :zookeeper-path zk-root-path}
                      nne))))))

(def digest-type 
  (BookKeeper$DigestType/MAC))

(defn password [peer-opts]
  (.getBytes ^String (arg-or-default :onyx.bookkeeper/ledger-password peer-opts)))

(defn new-ledger ^org.apache.bookkeeper.client.LedgerHandle [client peer-opts]
  (let [ensemble-size (arg-or-default :onyx.bookkeeper/ledger-ensemble-size peer-opts)
        quorum-size (arg-or-default :onyx.bookkeeper/ledger-quorum-size peer-opts)]
    (create-ledger client ensemble-size quorum-size digest-type (password peer-opts))))

