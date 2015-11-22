## onyx-bookkeeper

Onyx plugin providing read and write facilities for BookKeeper ledgers

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-bookkeeper "0.8.1.0-SNAPSHOT"]
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
 :bookkeeper/ledger-start-id <<OPTIONAL_LEDGER_START_INDEX>>
 :bookkeeper/ledger-end-id <<OPTIONAL_LEDGER_END_INDEX>>
 :bookkeeper/no-recovery? false
 :bookkeeper/digest-type :mac
 :checkpoint/force-reset? true
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
|`:bookkeeper/uri`                | `string`  | The URI of the bookkeeper database to connect to
|`:bookkeeper/log-start-tx`       | `integer` | optional starting tx (inclusive) for log read
|`:bookkeeper/log-end-tx`         | `integer` | optional ending tx (exclusive) for log read. Sentinel will emitted when this tx is passed.
|`:checkpoint/force-reset?`    | `boolean` | whether or not checkpointing should be re-initialised from log-start-tx, or 0 in the case of nil
|`:checkpoint/key`             | `any`     | optional global (for a given onyx/id) key under which to store the checkpoint information. By default the task-id for the job will be used, in which case checkpointing will only be resumed when a virtual peer crashes, and not when a new job is started.
|`:bookkeeper/read-buffer`        | `integer` | The number of segments to buffer after partitioning, default is `1000`

##### commit-tx

Writes new entity maps to bookkeeper. Will automatically assign tempid's for the partition
if a value for :bookkeeper/partition is supplied and bookkeeper transaction data is in map form.

Catalog entry:

```clojure
{:onyx/name :write-datoms
 :onyx/plugin :onyx.plugin.bookkeeper/write-datoms
 :onyx/type :output
 :onyx/medium :bookkeeper
 :bookkeeper/uri db-uri
 :bookkeeper/partition :my.database/optional-partition-name
 :onyx/batch-size batch-size
 :onyx/doc "Transacts segments to storage"}
```

Lifecycle entry:

```clojure
{:lifecycle/task :write-datoms
 :lifecycle/calls :onyx.plugin.bookkeeper/write-tx-calls}
```

###### Attributes

| key                          | type      | description
|------------------------------|-----------|------------
|`:bookkeeper/partition`          | `keyword` | Optional keyword. When supplied, :db/id tempids are added using this partition.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
