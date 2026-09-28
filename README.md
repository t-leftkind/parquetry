# parquetry

An idiomatic Clojure wrapper over [parquet-floor][pf], extracted from the
DM-2760 rev-share port so the next DE job can use it without depending on that
job.

[pf]: https://github.com/strategicblue/parquet-floor

| Namespace | Depends on | Purpose |
|---|---|---|
| `parquetry.core` | parquet-floor only | `write!` / `read` over Clojure maps. Schema is a plain vector of column descriptors. |
| `parquetry.malli` | malli | Derive those descriptors from a malli `:map`, so a dataset's shape is declared once. Optional. |

## Use

```clojure
;; deps.edn
io.github.t-leftkind/parquetry {:git/tag "v0.1.0" :git/sha "..."}
```

The published group id is deliberately unsettled — this library is dormant and
unpublished, so consumers use a git coordinate. See the repo-split design's
"Deferred decisions".

```clojure
(require '[parquetry.core :as pq]
         '[parquetry.malli :as pqm])

(def Sale
  [:map {:closed true}
   [:id       :int]
   [:sku      :string]
   [:note     [:maybe :string]]
   [:sold_at  pqm/instant]
   [:amount   (pqm/decimal 18 6)]])

(def descriptors (pqm/->column-descriptors Sale))

(pq/write! "sales.parquet" descriptors "sales" rows)   ; rows may be an eduction
(pq/read "sales.parquet")
```

`write!` consumes any `Iterable`, so a reducible over a live JDBC result set
streams straight to disk at constant memory. It does not realise its input —
a reducible must still be consumed inside the scope owning its connection.

## Column types

`:string`, `:long`, `:timestamp` (stored as `TIMESTAMP(MICROS)`), and
`:decimal` with `:precision` / `:scale`. `:null? true` makes a column optional;
columns are required by default, and `write!` rejects a row that omits a
required one rather than writing a structurally valid but unreadable file.

## Not Hadoop

`parquet-floor` pulls `org.apache.parquet/parquet-hadoop`, which is a *Parquet
project module*, not Apache Hadoop. No `hadoop-common`, no HDFS. The whole
parquet stack is ~8.8 MB across six jars.

## Test

```bash
clojure -M:test
```
