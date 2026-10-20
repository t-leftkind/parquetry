(ns parquetry.core
  "Idiomatic Clojure wrapper over parquet-floor.

  Callers work in Clojure maps with keyword keys, `BigDecimal`s, and `Instant`s
  (or `LocalDateTime`s, for a `:utc? false` timestamp column).
  Nothing outside this namespace should see a `MessageType`, an unscaled long,
  a `Dehydrator`, or a `String[]` column path.

  Schema is DATA (a vector of column maps), turning it into a `MessageType` is a
  CALCULATION, and only `write!` / `read` are ACTIONS.

  This namespace knows nothing about any particular dataset and depends on
  nothing but parquet-floor. It is a separate library rather than a namespace
  inside a job so that the reuse claim is enforced by the dependency graph
  instead of by good intentions: it CANNOT acquire a job-specific dependency
  without someone noticing.

  Three parquet-floor gotchas are handled here so no call site has to know them:

    1. `Dehydrator/dehydrate` returns void, so a `reify` body must end in `nil`
       or it will not compile.
    2. `Hydrator/add` receives the column key as a **String[] column path**, not
       a String. `(str k)` silently yields \"[Ljava.lang.String;@...\" and every
       lookup then returns nil with no error at all.
    3. Null values must be SKIPPED on write, not written as nil."
  (:refer-clojure :exclude [read])
  (:import (blue.strategic.parquet
             Dehydrator
             Hydrator
             ParquetReader
             ParquetWriter
             ValueWriter)
           java.io.File
           (java.math
             BigDecimal
             BigInteger
             RoundingMode)
           (java.time
             Instant
             LocalDateTime
             ZoneOffset)
           (org.apache.parquet.schema
             LogicalTypeAnnotation
             LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
             LogicalTypeAnnotation$TimeUnit
             LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
             LogicalTypeAnnotation$StringLogicalTypeAnnotation
             MessageType
             PrimitiveType
             PrimitiveType$PrimitiveTypeName
             Type$Repetition
             Types)))


;; ---------------------------------------------------------------- data
;;
;; A schema is a vector of column maps:
;;
;;   [{:name :tier              :type :string}
;;    {:name :total_revenue     :type :decimal :precision 18 :scale 6 :null? true}
;;    {:name :created_at        :type :timestamp}
;;    {:name :pg_created_at     :type :timestamp :utc? false}]
;;
;; :null? defaults to false (required). :utc? defaults to true: the column holds
;; instants (isAdjustedToUTC=true, `Instant`). `:utc? false` is a naive local
;; timestamp (isAdjustedToUTC=false, `LocalDateTime`), as Postgres `timestamp`
;; and pyarrow `timestamp("us")` store it.
;;
;; Decimals are stored as INT32 up to 9 digits and INT64 up to 18, as the parquet
;; spec recommends (parquet-java warns otherwise), so precision is at most 18.

(def ^:private binary PrimitiveType$PrimitiveTypeName/BINARY)
(def ^:private int32  PrimitiveType$PrimitiveTypeName/INT32)
(def ^:private int64  PrimitiveType$PrimitiveTypeName/INT64)
(def ^:private boolean-type PrimitiveType$PrimitiveTypeName/BOOLEAN)
(def ^:private max-int32-precision 9)
(def ^:private max-int64-precision 18)


;; ------------------------------------------------------- calculations

(defn- builder
  "Types/required or Types/optional for the given primitive."
  [primitive null?]
  (if null? (Types/optional primitive) (Types/required primitive)))


(defn- ->field
  "One column map -> a parquet Type. Pure."
  [{:keys [name type null? precision scale utc?] :or {utc? true}}]
  (let [col (clojure.core/name name)]
    (case type
      :string  (-> (builder binary null?) (.as (LogicalTypeAnnotation/stringType)) (.named col))
      :long    (-> (builder int64 null?) (.named col))
      :boolean (-> (builder boolean-type null?) (.named col))

      :decimal
      (do (when (> precision max-int64-precision)
            (throw (ex-info (str "decimal column " name " has precision " precision "; INT64 holds at most " max-int64-precision)
                            {:type ::unsupported-column-type :column name :precision precision})))
          (-> (builder (if (<= precision max-int32-precision) int32 int64) null?)
              ;; NOTE the argument order: decimalType(scale, precision).
              (.as (LogicalTypeAnnotation/decimalType (int scale) (int precision)))
              (.named col)))

      :timestamp (-> (builder int64 null?)
                     (.as (LogicalTypeAnnotation/timestampType
                            (boolean utc?) LogicalTypeAnnotation$TimeUnit/MICROS))
                     (.named col))
      (throw (ex-info "unsupported parquet column type"
                      {:type ::unsupported-column-type :column name :given type})))))


(defn- add-field
  "Append one column spec to a MessageType builder."
  [builder col]
  (.addField builder (->field col)))


(defn message-type
  "Schema data -> MessageType. Pure."
  ^MessageType [schema root]
  (-> (reduce add-field (Types/buildMessage) schema)
      (.named root)))


(defn decimal->unscaled
  "BigDecimal -> the unscaled long parquet stores. Pure. HALF_EVEN by default;
  RoundingMode/UNNECESSARY throws ArithmeticException rather than round."
  (^long [v scale] (decimal->unscaled v scale RoundingMode/HALF_EVEN))
  (^long [v scale ^RoundingMode mode]
   (-> (BigDecimal. (str v))
       (.setScale (int scale) mode)
       (.unscaledValue)
       (.longValueExact))))


(defn unscaled->decimal
  "The unscaled long parquet stores -> BigDecimal. Pure."
  ^BigDecimal [raw scale]
  (BigDecimal. (BigInteger/valueOf (long raw)) (int scale)))


(defn instant->micros
  "Instant -> epoch microseconds, the physical form of a TIMESTAMP(MICROS)
  column. Passes a long straight through so callers may supply either. Pure."
  ^long [v]
  (if (instance? Instant v)
    (+ (* (.getEpochSecond ^Instant v) 1000000)
       (long (/ (.getNano ^Instant v) 1000)))
    (long v)))


(defn micros->instant
  "Epoch microseconds -> Instant. Pure."
  ^Instant [v]
  (let [micros (long v)]
    (Instant/ofEpochSecond (quot micros 1000000) (* (rem micros 1000000) 1000))))


(defn local-date-time->micros
  "LocalDateTime -> microseconds since the epoch, read as wall-clock time, the
  physical form of a naive TIMESTAMP(MICROS). Passes a long straight through. Pure."
  ^long [v]
  (if (instance? LocalDateTime v)
    (+ (* (.toEpochSecond ^LocalDateTime v ZoneOffset/UTC) 1000000)
       (quot (.getNano ^LocalDateTime v) 1000))
    (long v)))


(def ^:private units-per-second
  {LogicalTypeAnnotation$TimeUnit/MILLIS 1000
   LogicalTypeAnnotation$TimeUnit/MICROS 1000000
   LogicalTypeAnnotation$TimeUnit/NANOS  1000000000})


(defn- epoch->time
  "A stored timestamp in `unit`s -> Instant, or LocalDateTime when not `utc?`. Pure."
  [utc? unit v]
  (let [per   (long (units-per-second unit))
        secs  (Math/floorDiv (long v) per)
        nanos (* (Math/floorMod (long v) per) (quot 1000000000 per))]
    (if utc?
      (Instant/ofEpochSecond secs nanos)
      (LocalDateTime/ofEpochSecond secs (int nanos) ZoneOffset/UTC))))


(defn- exact-decimal
  "A strict decimal encoder: refuses a value that would need rounding, naming the column."
  [name scale]
  (fn [v]
    (try
      (decimal->unscaled v scale RoundingMode/UNNECESSARY)
      (catch ArithmeticException e
        (throw (ex-info (str "decimal column " name " cannot hold " v " at scale " scale " without rounding")
                        {:type ::inexact-decimal :column name :value v :scale scale}
                        e))))))


(defn- ->int32
  ;; An Integer, not a long: parquet-floor's INT32 writer casts to Integer.
  [v]
  (Integer/valueOf (Math/toIntExact (long v))))


(defn- encoder
  "Column map -> fn coercing a Clojure value to what parquet-floor writes. Pure."
  [{:keys [name type precision scale utc?] :or {utc? true}} round-decimals?]
  (case type
    :decimal   (let [unscaled (if round-decimals? #(decimal->unscaled % scale) (exact-decimal name scale))]
                 (if (<= precision max-int32-precision)
                   (comp ->int32 unscaled)
                   unscaled))
    :timestamp (if utc? instant->micros local-date-time->micros)
    identity))


(defn- decoder
  "A parquet field -> fn turning the stored value back into a Clojure value.

  Derived from the FILE's own logical type annotation rather than from a schema
  the caller passes in, so a read can never disagree with what was written --
  including files from other writers. pyarrow stores decimals as
  FIXED_LEN_BYTE_ARRAY, which parquet-floor hands over as decimal text, and may
  store timestamps in MILLIS or NANOS."
  [^PrimitiveType field]
  (let [ann (.getLogicalTypeAnnotation field)]
    (condp instance? ann
      LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
      (let [scale (int (.getScale ^LogicalTypeAnnotation$DecimalLogicalTypeAnnotation ann))]
        #(if (string? %)
           (.setScale (BigDecimal. ^String %) scale)
           (unscaled->decimal % scale)))

      LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
      (let [^LogicalTypeAnnotation$TimestampLogicalTypeAnnotation t ann]
        (partial epoch->time (.isAdjustedToUTC t) (.getUnit t)))

      identity)))


(def ^:private unit-names
  {LogicalTypeAnnotation$TimeUnit/MILLIS :millis
   LogicalTypeAnnotation$TimeUnit/NANOS  :nanos})


(defn- field->column
  "A parquet field -> a column map in `write!`'s schema terms. Pure. Defaults are
  left out, as in a hand-written schema: :null? appears only when true, :utc? only
  when false, :unit only when not micros. Anything else is {:type :unknown}."
  [^PrimitiveType f]
  (let [ann  (.getLogicalTypeAnnotation f)
        base {:name (keyword (.getName f))}
        col  (condp instance? ann
               LogicalTypeAnnotation$StringLogicalTypeAnnotation
               (assoc base :type :string)

               LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
               (let [^LogicalTypeAnnotation$DecimalLogicalTypeAnnotation d ann]
                 (assoc base :type :decimal :precision (.getPrecision d) :scale (.getScale d)))

               LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
               (let [^LogicalTypeAnnotation$TimestampLogicalTypeAnnotation t ann
                     unit (unit-names (.getUnit t))]
                 (cond-> (assoc base :type :timestamp)
                   (not (.isAdjustedToUTC t)) (assoc :utc? false)
                   unit                       (assoc :unit unit)))

               (cond
                 (and (nil? ann) (= int64 (.getPrimitiveTypeName f)))        (assoc base :type :long)
                 (and (nil? ann) (= boolean-type (.getPrimitiveTypeName f))) (assoc base :type :boolean)
                 :else                                                       (assoc base :type :unknown :parquet (str f))))]
    (cond-> col
      (= Type$Repetition/OPTIONAL (.getRepetition f)) (assoc :null? true))))


;; ------------------------------------------------------------ actions

(defn- file-message-type
  ^MessageType [path]
  (.getSchema (.getFileMetaData (ParquetReader/readMetadata (File. (str path))))))


(defn write!
  "Write `rows` (maps with keyword keys) to `path`. Returns the row count.

  Nulls are skipped rather than written -- parquet-floor requires this for
  optional columns. Decimals are rounded HALF_EVEN to the column's scale unless
  `:round-decimals? false`, which throws {:type ::inexact-decimal} instead. A
  missing required value throws {:type ::missing-required}."
  ([path schema root rows] (write! path schema root rows {}))
  ([path schema root rows {:keys [round-decimals?] :or {round-decimals? true}}]
   (let [mt       (message-type schema root)
         encoders (into {} (map (juxt :name #(encoder % round-decimals?))) schema)
         cols     (mapv :name schema)
         ;; Columns the schema declares non-optional. Skipping one of these
         ;; produces a file that is structurally valid and unreadable.
         required (into #{} (comp (remove :null?) (map :name)) schema)
         n        (volatile! 0)]
     (with-open [w (ParquetWriter/writeFile
                     mt (File. (str path))
                     (reify Dehydrator
                       (dehydrate
                         [_ row vw]
                         (let [^ValueWriter vw vw]
                           (doseq [k cols
                                   :let [v (get row k)]]
                             (if (some? v)
                               (.write vw (name k) ((encoders k) v))
                               ;; Skipping nils is REQUIRED by parquet-floor for
                               ;; optional columns -- but it makes a total key
                               ;; mismatch indistinguishable from "every value
                               ;; is null". A run where the row maps used
                               ;; next.jdbc's default QUALIFIED keys
                               ;; (:WIH_USER_SESSION/ID) rather than :id wrote
                               ;; 174,128 rows in which no column had a single
                               ;; value: a 6 KB file whose footer claimed the
                               ;; full row count. It failed in the NEXT step,
                               ;; on read, as
                               ;;   Can't read value in column [id] ...
                               ;;   no more value to read, total value count is 0
                               ;; which points at the reader rather than at the
                               ;; writer that produced the nonsense.
                               ;;
                               ;; So: required columns fail here, loudly, naming
                               ;; the column and the keys the row actually had.
                               (when (contains? required k)
                                 (throw (ex-info
                                          (str "parquet write: required column " k
                                               " is missing or nil")
                                          {:type     ::missing-required
                                           :column   k
                                           :row-keys (vec (sort (map str (keys row))))})))))
                           ;; MUST end in nil: dehydrate returns void.
                           nil))))]
       (doseq [row rows]
         (.write w row)
         (vswap! n inc)))

     @n)))


(defn read
  "Read `path` into a vector of maps with keyword keys.

  Decoding is driven by the file's own schema, so decimals come back as
  `BigDecimal` and timestamps as `Instant` (or `LocalDateTime`, when the file
  says isAdjustedToUTC=false) without the caller supplying anything."
  [path]
  (let [f        (File. (str path))
        mt       (file-message-type path)
        decoders (into {} (for [^PrimitiveType fld (.getFields mt)]
                            [(.getName fld) (decoder fld)]))]
    (with-open [s (ParquetReader/streamContent
                    f (reify Hydrator
                        (start [_] (transient {}))

                        (add
                          [_ acc k v]
                          ;; k is a String[] column PATH, not a String.
                          ;; parquet-floor calls add with nil for an absent
                          ;; optional column, so the decoder must be skipped --
                          ;; otherwise a NULL decimal NPEs on decode.
                          (let [col (last (vec k))]
                            (assoc! acc (keyword col)
                                    (when (some? v) ((decoders col) v)))))

                        (finish [_ acc] (persistent! acc))))]
      (vec (.toList s)))))


(defn file-schema
  "The MessageType recorded in the file, as a string. For contract assertions."
  [path]
  (str (file-message-type path)))


(defn file-columns
  "The file's columns as a `write!` schema, read from the footer. ACTION.

  Compares logically: two files with the same columns describe the same, whatever
  physical encoding each writer chose for a decimal. So a contract check is
  `(= expected-schema (file-columns path))`."
  [path]
  (mapv field->column (.getFields (file-message-type path))))
