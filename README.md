# parquetry

An idiomatic Clojure wrapper over [parquet-floor][pf], extracted from the
DM-2760 rev-share port so the next DE job can use it without depending on that
job.

[pf]: https://github.com/strategicblue/parquet-floor

| Namespace | Depends on | Purpose |
|---|---|---|
| `parquetry.core` | parquet-floor, Truss | `write!` / `read` over Clojure maps. Schema is a plain vector of column descriptors. |
| `parquetry.inspect` | `parquetry.core` | Check an artifact's footer against its actual per-column fill. |
| `parquetry.malli` | malli | Derive those descriptors from a malli `:map`, so a dataset's shape is declared once. Optional. |

`parquetry.core` is built from `schema` (columns ↔ parquet fields), `codec`
(per-column encoders and decoders), `convert` (decimal and timestamp
conversions, JDK only) and `floor` (all parquet-floor interop). Callers normally
need only `core`.

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

`write!` reduces its rows, so it accepts any reducible, and a reducible over a live JDBC result set
streams straight to disk at constant memory. It does not realise its input —
a reducible must still be consumed inside the scope owning its connection.

`read` returns every row at once. `(pq/rows path)` is the same rows as a
reducible, decoded one at a time: `(into [] xf (pq/rows path))` holds only what
`xf` keeps. Each reduce opens the file and closes it when done.

## Column types

`:string`, `:long`, `:boolean`, `:timestamp` (stored as `TIMESTAMP(MICROS)`), and
`:decimal` with `:precision` / `:scale` (stored as INT32 up to 9 digits and
INT64 up to 18, as the parquet spec recommends, so precision ≤ 18).
`:null? true` makes a column optional; columns are required by default, and
`write!` rejects a row that omits a required one rather than writing a
structurally valid but unreadable file. A malformed column (no `:name`, a
decimal without `:precision` or `:scale`) fails a Truss assertion whose `:data`
holds the column.

A timestamp column holds `Instant`s (isAdjustedToUTC=true) unless it says
`:utc? false`, which makes it a naive wall-clock timestamp holding
`LocalDateTime`s, as Postgres `timestamp` stores it. In malli, `pqm/instant` and
`pqm/local-date-time`.

Decimals are rounded HALF_EVEN to the column's scale. Pass
`{:round-decimals? false}` as `write!`'s last argument to throw
`{:type ::pq/inexact-decimal}` instead.

## Reading other writers' files

`read` decodes from the file's own footer, so it reads files parquetry did not
write: pyarrow's FIXED_LEN_BYTE_ARRAY decimals and MILLIS or NANOS timestamps
included. `file-columns` describes a file in the same terms as a `write!`
schema, physical encoding aside, so a contract check is
`(= schema (pq/file-columns path))`. `test/fixtures/make_fixtures.py` writes the
pyarrow file the tests read.

## Not Hadoop

`parquet-floor` pulls `org.apache.parquet/parquet-hadoop`, which is a *Parquet
project module*, not Apache Hadoop. No `hadoop-common`, no HDFS. The whole
parquet stack is ~8.8 MB across six jars.

## Test

```bash
clojure -M:test
clojure -M:splint src test   # style lint; CI runs both
```
