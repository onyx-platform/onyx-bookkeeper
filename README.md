## onyx-bookkeeper

Onyx plugin providing read and write facilities for BookKeeper ledgers

#### Installation 

In your project file:

```clojure
[org.onyxplatform/onyx-bookkeeper "0.10.0.0-beta10"]
```
In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.bookkeeper])
```

#### Functions

##### read-log

Reads the bookkeeper ledger log via repeated chunked calls ...

Catalog entry:

```clojure
{:onyx/name :read-log
 :onyx/plugin :onyx.plugin.bookkeeper/read-ledgers
 :onyx/type :input
 :onyx/medium :bookkeeper
 :bookkeeper/zookeeper-addr <<ZOOKEEPER_ADDR>>
 :bookkeeper/ledger-start-id <<OPTIONAL_LEDGER_START_INDEX>>
 :bookkeeper/ledger-end-id <<OPTIONAL_LEDGER_END_INDEX>>
 :bookkeeper/no-recovery? false
 :bookkeeper/digest-type :mac
 :bookkeeper/deserializer-fn :your-ns/your-deserializer-fn
 :bookkeeper/password-bytes (.getBytes "YOURPASSWORD")
 :onyx/max-peers 1
 :onyx/batch-size batch-size
 :onyx/doc "Reads a sequence of entries from a BookKeeper ledger"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :read-log
 :lifecycle/calls :onyx.plugin.bookkeeper/read-ledgers-calls}
```

Task will emit a sentinel `:done` when it reaches the ledger-end-id, or the end of a closed ledger.

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:bookkeeper/zookeeper-addr`  | `string`  | ZooKeeper address for the ensemble
|`:bookkeeper/ledger-start-id` | `integer` | optional starting id for ledger
|`:bookkeeper/ledger-end-id`   | `integer` | optional ending id for ledger
|`:bookkeeper/no-recovery?`    | `boolean` | open non-closed ledger. Job will be completed when ledger is closed
|`:bookkeeper/digest-type`     | `enum`    | BookKeeper digest, either :mac or :crc32
|`:bookkeeper/deserializer-fn` | `keyword` | Namespaced keyword pointing to deserialization function
|`:bookkeeper/password-bytes`  | `bytes`   | Password to open the ledger
|`:checkpoint/force-reset?`    | `boolean` | whether or not checkpointing should be re-initialised from log-start-tx, or 0 in the case of nil
|`:checkpoint/key`             | `any`     | optional global (for a given onyx/tenancy-id) key under which to store the checkpoint information. By default the task-id for the job will be used, in which case checkpointing will only be resumed when a virtual peer crashes, and not when a new job is started.
|`:bookkeeper/read-buffer`     | `integer` | The number of segments to buffer after partitioning, default is `1000`
|`:bookkeeper/read-max-chunk-size` | `integer` | optional chunk size to read from the ledger at a time

##### write-ledger

Writes new data to a newly created BookKeeper ledger. 

Catalog entry:

```clojure
{:onyx/name :write-ledger
 :onyx/plugin :onyx.plugin.bookkeeper/write-ledger
 :onyx/type :output
 :onyx/medium :bookkeeper
 :bookkeeper/zookeeper-addr <<ZOOKEEPER_ADDR>>
 :bookkeeper/digest-type :mac
 :bookkeeper/serializer-fn :your-ns/your-serializer-fn
 :bookkeeper/password-bytes (.getBytes "YOURPASSWORD")
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-ledger
 :lifecycle/calls :onyx.plugin.bookkeeper/write-ledger-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:bookkeeper/zookeeper-addr`  | `string`  | ZooKeeper address for the ensemble
|`:bookkeeper/digest-type`     | `enum`    | BookKeeper digest, either :mac or :crc32
|`:bookkeeper/serializer-fn`   | `keyword` | Namespaced keyword pointing to serialization function
|`:bookkeeper/password-bytes`  | `bytes`   | Password to open the ledger

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
