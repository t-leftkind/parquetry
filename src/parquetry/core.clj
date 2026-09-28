(ns parquetry.core
  "Idiomatic Clojure wrapper over parquet-floor.

  Callers work in Clojure maps with keyword keys, `BigDecimal`s, and `Instant`s.
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
           java.time.Instant
           (org.apache.parquet.schema
             LogicalTypeAnnotation
             LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
             LogicalTypeAnnotation$TimeUnit
             LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
             MessageType
             PrimitiveType
             PrimitiveType$PrimitiveTypeName
             Types)))


;; ---------------------------------------------------------------- data
;;
;; A schema is a vector of column maps:
;;
;;   [{:name :tier              :type :string}
;;    {:name :total_revenue     :type :decimal :precision 18 :scale 6 :null? true}
;;    {:name :created_at        :type :timestamp}]
;;
;; :null? defaults to false (required).

(def ^:private binary PrimitiveType$PrimitiveTypeName/BINARY)
(def ^:private int64  PrimitiveType$PrimitiveTypeName/INT64)


;; ------------------------------------------------------- calculations

(defn- builder
  "Types/required or Types/optional for the given primitive."
  [primitive null?]
  (if null? (Types/optional primitive) (Types/required primitive)))


(defn- ->field
  "One column map -> a parquet Type. Pure."
  [{:keys [name type null? precision scale]}]
  (let [col (clojure.core/name name)]
    (case type
      :string (-> (builder binary null?) (.as (LogicalTypeAnnotation/stringType)) (.named col))
      :long   (-> (builder int64 null?) (.named col))
      :decimal (-> (builder int64 null?)
                   ;; NOTE the argument order: decimalType(scale, precision).
                   (.as (LogicalTypeAnnotation/decimalType (int scale) (int precision)))
                   (.named col))
      :timestamp (-> (builder int64 null?)
                     (.as (LogicalTypeAnnotation/timestampType
                            true LogicalTypeAnnotation$TimeUnit/MICROS))
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
  "BigDecimal -> the unscaled long parquet stores. Pure. HALF_EVEN, per contract."
  ^long [v scale]
  (-> (BigDecimal. (str v))
      (.setScale (int scale) RoundingMode/HALF_EVEN)
      (.unscaledValue)
      (.longValueExact)))


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


(defn- encoder
  "Column map -> fn coercing a Clojure value to what parquet-floor writes. Pure."
  [{:keys [type scale]}]
  (case type
    :decimal   #(decimal->unscaled % scale)
    :timestamp instant->micros
    identity))


(defn- decoder
  "A parquet field -> fn turning the stored value back into a Clojure value.

  Derived from the FILE's own logical type annotation rather than from a schema
  the caller passes in, so a read can never disagree with what was written."
  [^PrimitiveType field]
  (let [ann (.getLogicalTypeAnnotation field)]
    (condp instance? ann
      LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
      (let [scale (.getScale ^LogicalTypeAnnotation$DecimalLogicalTypeAnnotation ann)]
        #(unscaled->decimal % scale))

      LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
      micros->instant

      identity)))


;; ------------------------------------------------------------ actions

(defn write!
  "Write `rows` (maps with keyword keys) to `path`. Returns the row count.

  Nulls are skipped rather than written -- parquet-floor requires this for
  optional columns."
  [path schema root rows]
  (let [mt       (message-type schema root)
        encoders (into {} (map (juxt :name encoder)) schema)
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
                                         {:column   k
                                          :row-keys (vec (sort (map str (keys row))))})))))
                          ;; MUST end in nil: dehydrate returns void.
                          nil))))]
      (doseq [row rows]
        (.write w row)
        (vswap! n inc)))
    @n))


(defn read
  "Read `path` into a vector of maps with keyword keys.

  Decoding is driven by the file's own schema, so decimals come back as
  `BigDecimal` and timestamps as `Instant` without the caller supplying
  anything."
  [path]
  (let [f        (File. (str path))
        mt       (.getSchema (.getFileMetaData (ParquetReader/readMetadata f)))
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
  (str (.getSchema (.getFileMetaData (ParquetReader/readMetadata (File. (str path)))))))
