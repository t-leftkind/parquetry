# Clojure Parquet Spike — Findings

**Date:** 2026-09-09
**JDK:** openjdk 21.0.11 (Homebrew `openjdk@21`)
**Gate for:** plan 3, the Clojure port

## Verdict: parquet-floor

`blue.strategic.parquet/parquet-floor {:mvn/version "2.3"}`

Reproduce with `clojure -M:floor` (or `-M:floor-full` for the real schema);
other candidates are `-M:tmd`, `-M:pqjava`, `-M:duckdb`.

Three candidates pass, one fails. `tmd-parquet` cannot represent a decimal at
all and is out. The other three all round-trip `DECIMAL(18,6)` correctly, so
the choice is about weight, dependency shape, and independence:

| | direct deps | jars | size | native lib | Hadoop | parquet module API |
|---|---:|---:|---:|---|---|---|
| **`parquet-floor` 2.3** | **1** | 14 | **23 MB** | no | **none** | Dehydrator / Hydrator |
| `duckdb_jdbc` 1.3.1.0 | 1 | 1 | 73 MB | yes | none | two SQL statements |
| `parquet-java` 1.15.2 | 3 | 17 | 68 MB | no | shaded client jars | Group API |

`parquet-floor` wins on every axis that is not a tie. It is a thin wrapper over
the canonical `parquet-column` / `parquet-hadoop` 1.18.1 — so the actual encoder
is the reference implementation — but it takes a plain `java.io.File` and pulls
in no Hadoop whatsoever. At 23 MB it is roughly a third the size of either
alternative.

### It handles the real schema, not just the happy path

Probed against the full `contracts/rows.schema.sql` shape, including the cases
that matter:

```
message rows {
  required binary tier (STRING);
  required int64 id;
  optional binary aff_sub (STRING);
  optional binary aff_sub2 (STRING);
  optional binary aff_id (STRING);
  optional binary ha_offer_id (STRING);
  required binary ha_transaction_id (STRING);
  required int64 created_at (TIMESTAMP(MICROS,true));
  optional int64 total_revenue (DECIMAL(18,6));
}
PASS: nullable decimal, nullable strings, and timestamp all round-trip
```

The **nullable** `DECIMAL(18,6)` is the important one — `total_revenue` is
nullable by design (see `contracts/README.md`) and reads back as `nil`.

### It also removes the blind spot DuckDB would have carried

Written by `parquet-floor`, then read by DuckDB as a genuinely independent
engine:

```
┌───────────────────┬───────────────┬──────────────────────────┐
│ ha_transaction_id │ total_revenue │        created_at        │
│      varchar      │ decimal(18,6) │ timestamp with time zone │
├───────────────────┼───────────────┼──────────────────────────┤
│ tx-1              │      1.500000 │ 2026-08-29 03:40:00-07   │
│ tx-2              │          NULL │ 2026-08-29 03:40:03.6-07 │
└───────────────────┴───────────────┴──────────────────────────┘
```

Had the port both written and verified with DuckDB, a DuckDB-specific encoding
quirk would have passed its own contract test. Writing with parquet-floor and
asserting with DuckDB's `DESCRIBE` mirrors the Python port exactly, which writes
with pyarrow and asserts with DuckDB.

### Two API gotchas, both hit during the spike

- `Dehydrator/dehydrate` returns `void`. A Clojure `reify` body must end in
  `nil` or it fails to compile with `Mismatched return type ... expected: void`.
- `Hydrator/add` receives the column key as a **`String[]` column path**, not a
  `String`. `(str k)` silently yields `[Ljava.lang.String;@...` and every lookup
  returns `nil` with no error. Use `(last (vec k))`.
- Null fields must be **skipped** rather than written — iterate with
  `:when (some? v)`.

### Residual risk

`parquet-floor` is a small third-party wrapper rather than a first-party
artifact, so it carries more bus-factor risk than `parquet-java` or DuckDB. The
mitigation is that it is genuinely thin: if it were ever abandoned, the escape
hatch is the `parquet-java` path recorded below, which is already proven to work
here and would be a contained change to one action module.

---|---:|---:|---:|---|---|
| `duckdb_jdbc` | **1** | 1 | 73 MB | yes | two SQL statements |
| `parquet-java` | 3 | 17 | **68 MB** | no | Group API, column-by-column |

DuckDB is selected for the smaller direct-dependency count, the much smaller
parquet module, and because the same engine already backs the contract test and
plan 4's comparison. parquet-java's real advantages are that it is the canonical
implementation, it is pure JVM with no native binary to worry about across
architectures, and it avoids the blind spot below.

**Known blind spot in the DuckDB choice:** the Clojure port would both write its
parquet with DuckDB and have its contract test verify that parquet with DuckDB,
so a DuckDB-specific encoding quirk would pass its own check. The Python port
does not share this — it writes with pyarrow and verifies with DuckDB. This is
mitigated but not eliminated by plan 4's CSV comparison against the Airflow
oracle. If the two ports ever agree with each other and disagree with the
oracle, look here first.

---

## Candidate 1 — `com.techascent/tmd-parquet` 1.001 · REJECTED

The spec named this the primary choice. It cannot write a decimal column at all.

```
Execution error at tech.v3.libs.parquet/column->field (parquet.clj:928).
Unsupported datatype for parquet writing: :decimal
```

Confirmed by the library's own documentation, which lists the supported
write types as "all numeric types, strings, `java.time.LocalDate`, `Instant`,
UUIDs" — `BigDecimal` is absent, so this is a designed limitation and not a
version or configuration problem.

`DECIMAL(18,6)` is the entire basis of the cross-language number contract
(`contracts/number-vectors.json`), so a library that cannot represent it is
disqualified outright.

Two coordinate corrections while we are here — plan 1 guessed both wrong and
they should not be copied forward: the artifact is on **Clojars, not Maven
Central**, and its latest version is **1.001, not 1.003**. Latest
`techascent/tech.ml.dataset` is **8.026, not 7.032**.

### Also considered: `tmducken`

Same maintainer, binds DuckDB to `tech.ml.dataset` over FFI. Rejected without
a full probe for two reasons: it "requires the user to install/manage the
duckdb binary dependency" via `DUCKDB_HOME` or a nix build, which is real
recurring work in a container image, and it sits on the same
`tech.ml.dataset` type model that lacks `BigDecimal` above. It is also a
DuckDB *binding*, not a parquet writer, so it does not address the actual
requirement. `duckdb_jdbc` bundles its native library in the jar and needs
none of that.

---

## Candidate 2 — `org.apache.parquet/parquet-hadoop` 1.15.2 · VIABLE (initially misjudged)

**Correction, 2026-09-09:** this candidate was first recorded as REJECTED on
the grounds that reading required Hadoop. That was an incomplete conclusion.
Reading needs Hadoop *classes*, which `hadoop-client-runtime` supplies as a
single shaded jar — it does not need a Hadoop cluster, a filesystem service, or
`hadoop-common`'s dependency explosion. With that jar present and a
`file://`-scheme `org.apache.hadoop.fs.Path`, the full decimal round-trip
passes:

```
wrote 614 bytes
  1.500000               -> 1.500000               ok
  0.300000               -> 0.300000               ok
  123.456789             -> 123.456789             ok
  0.000000               -> 0.000000               ok
  999999999999.999999    -> 999999999999.999999    ok
PASS: parquet-java decimal round-trip
```

Weight with `parquet-hadoop` + `hadoop-client-api` + `hadoop-client-runtime`:
**17 jars, 68 MB** — nearly identical in size to DuckDB's single 73 MB jar, but
three direct dependencies instead of one, and pure JVM with no native library.

What follows is the original analysis, which remains accurate about the API
shape and about why `hadoop-common` must be avoided.

**Write:** succeeded with no Hadoop filesystem involved, using
`LocalOutputFile` + `PlainParquetConfiguration` and an `INT64`-backed
`DECIMAL(18,6)` logical type. 614 bytes written.

**Read:** blocked. Reflection on the shipped jar shows the Example API exposes
exactly one reader entry point:

```
builder [ReadSupport Path]        <- org.apache.hadoop.fs.Path
withConf [Configuration]
withConf [ParquetConfiguration]
```

There is no `builder(ReadSupport, InputFile)` overload, so reading through the
Group API requires a Hadoop `Path` and therefore a Hadoop `FileSystem`
implementation at runtime. Passing a `LocalInputFile` fails:

```
ClassCastException: class org.apache.parquet.io.LocalInputFile cannot be cast
to class org.apache.hadoop.fs.Path
```

The escape hatch is the lower-level `ParquetFileReader` + column IO API, which
means hand-rolling record assembly — substantially more code in the module the
scorecard measures by LOC.

A second, Clojure-specific friction: `ExampleParquetWriter/builder` has a
Hadoop-typed overload, and Clojure resolves overloads by loading every
parameter type. Even the "Hadoop-free" write path throws
`ClassNotFoundException: org.apache.hadoop.fs.Path` until a Hadoop jar is on
the classpath purely to satisfy reflection.

**Dependency weight:**

| Configuration | Jars | Size |
|---|---:|---:|
| `parquet-hadoop` alone | 12 | 19 MB |
| `+ hadoop-client-api` (needed for the above) | 14 | 39 MB |
| `+ hadoop-common` (needed for the Path read path) | 60+ | drags in Jetty, Guava, servlet-api, httpclient |

---

## Candidate 3 — `org.duckdb/duckdb_jdbc` 1.3.1.0 · SELECTED

```
wrote 853 bytes
--- schema as written to the file ---
  conversion_id    VARCHAR
  payout           DECIMAL(18,6)
  revenue          DECIMAL(18,6)
--- round-trip values ---
  1.500000               -> 1.500000               scale=6 ok
  0.300000               -> 0.300000               scale=6 ok
  123.456789             -> 123.456789             scale=6 ok
  0.000000               -> 0.000000               scale=6 ok
  999999999999.999999    -> 999999999999.999999    scale=6 ok
PASS: duckdb_jdbc DECIMAL(18,6) parquet round-trip, scale preserved
```

The written file's own schema reports `DECIMAL(18,6)`, which is what
`contracts/rows.schema.sql` and `contracts/updates.schema.sql` require and
what plan 2's `DESCRIBE`-based contract test asserts. Scale survives the round
trip at every magnitude including the `DECIMAL(18,6)` maximum.

**Dependency weight: one jar, zero transitive dependencies, 73 MB.**

The size is the native DuckDB engine for all platforms, bundled. That is worse
than parquet-java's 19 MB on the scorecard's image-size dimension and better
on its dependency-count dimension — one direct dependency against twelve.

### Why this is the right trade despite being the largest jar

- It is the only candidate that satisfies the contract at all. Correctness is
  a gate, not a score.
- `COPY ... TO '...' (FORMAT PARQUET)` and `read_parquet(...)` are two lines of
  SQL. The parquet-java read path is a hand-rolled record assembler.
- The comparison step in plan 4 already uses DuckDB, so the same engine spans
  the port and its verification.
- Docker images can be slimmed later by stripping the non-Linux native
  libraries out of the jar; the dependency graph cannot be un-tangled later.

---

## JVM tuning implications for plan 3

Measured on this spike, JDK 21.0.11, 3 runs each, median wall clock:

| Flags | Median | Min |
|---|---:|---:|
| default | 1.72s | 1.67s |
| `-XX:TieredStopAtLevel=1 -XX:+UseSerialGC` (the spec's proposal) | 1.64s | 1.63s |
| `-XX:+UseSerialGC` only | 1.71s | 1.69s |
| `-XX:+UseSerialGC` + AppCDS auto-archive | 1.78s | 1.74s |

**Startup flag tuning is worth about 5% here — roughly 0.08s — and the spec is
optimising the wrong thing.** Three corrections:

1. **Drop `-XX:TieredStopAtLevel=1` for `fetch` and `lookup`.** It caps the JIT
   at C1. That is the right trade for a sub-second CLI, but these steps stream
   thousands of rows and make thousands of HTTP calls over minutes, where C2 is
   worth far more than the ~80ms of warmup it saves. Keep it only for `dump`,
   which is genuinely short.

2. **Size the heap as a percentage, not a fixed `-Xmx`, and leave native
   headroom.** The pods have `limits.memory: 2Gi`. Use
   `-XX:MaxRAMPercentage=50` rather than the usual 75, because **DuckDB
   allocates off-heap**: its native engine sits outside anything the JVM
   accounts for, so a heap sized at 75% of the limit leaves too little and the
   pod gets OOMKilled by the kubelet rather than throwing a Java OOM. Also cap
   DuckDB itself with `SET memory_limit='512MB'` so the two budgets are
   explicit and add up to less than the container limit.

3. **Keep `-XX:+UseSerialGC`, but for the CPU reason rather than the lifetime
   reason.** The plan sets `limits.cpu: "1"`. G1 assumes multiple cores for
   concurrent phases and is counterproductive on a one-CPU cgroup; SerialGC is
   correct here. If the CPU limit is ever raised above 2, revisit.

Also add `-XX:+ExitOnOutOfMemoryError` so a heap exhaustion kills the pod and
lets Argo's `retryStrategy` handle it, instead of the JVM thrashing in GC while
the step appears to hang. `-XX:+UseContainerSupport` is on by default in 21 and
does not need stating.

AppCDS measured slightly *worse* here and is not worth the Dockerfile
complexity at this classpath size; revisit only if cold start becomes a
scorecard differentiator.
