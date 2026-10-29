(ns parquetry.codec
  "Per-column value encoders and decoders. CALCULATIONS."
  (:require
    [parquetry.convert :as convert]
    [parquetry.schema :as schema])
  (:import
    java.math.RoundingMode
    (org.apache.parquet.schema
      LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
      LogicalTypeAnnotation$TimeUnit
      LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
      PrimitiveType)))


(defn- exact-decimal
  [col-name scale]
  (fn [v]
    (try
      (convert/decimal->unscaled v scale RoundingMode/UNNECESSARY)
      (catch ArithmeticException e
        (throw (ex-info (str "decimal column " col-name " cannot hold " v " at scale " scale " without rounding")
                        {:type   :parquetry.core/inexact-decimal
                         :column col-name
                         :value  v
                         :scale  scale}
                        e))))))


(defn- ->int32
  ;; An Integer, not a long: parquet-floor's INT32 writer casts to Integer.
  [v]
  (Integer/valueOf (Math/toIntExact (long v))))


(defn- decimal-encoder
  [{col-name :name :keys [scale] :as column} round-decimals?]
  (let [unscaled (if round-decimals?
                   #(convert/decimal->unscaled % scale)
                   (exact-decimal col-name scale))]
    (if (schema/int32-decimal? column)
      (comp ->int32 unscaled)
      unscaled)))


(defn encoder
  "Column -> fn coercing a Clojure value to what parquet-floor writes. With
  `round-decimals?` false, the fn throws {:type :parquetry.core/inexact-decimal}
  on a decimal that would need rounding."
  [{col-type :type :keys [utc?] :or {utc? true} :as column} round-decimals?]
  (case col-type
    :decimal   (decimal-encoder column round-decimals?)
    :timestamp (if utc? convert/instant->micros convert/local-date-time->micros)
    identity))


(def ^:private units-per-second
  {LogicalTypeAnnotation$TimeUnit/MILLIS 1000
   LogicalTypeAnnotation$TimeUnit/MICROS 1000000
   LogicalTypeAnnotation$TimeUnit/NANOS  1000000000})


(defn decoder
  "A parquet field -> fn turning the stored value back into a Clojure value.

  Derived from the file's own annotation rather than a caller's schema, so it
  reads other writers' files: pyarrow stores decimals as FIXED_LEN_BYTE_ARRAY,
  which parquet-floor hands over as decimal text, and timestamps in any unit."
  [^PrimitiveType field]
  (let [ann (.getLogicalTypeAnnotation field)]
    (condp instance? ann
      LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
      (let [scale (.getScale ^LogicalTypeAnnotation$DecimalLogicalTypeAnnotation ann)]
        #(if (string? %)
           (.setScale (BigDecimal. ^String %) scale)
           (convert/unscaled->decimal % scale)))

      LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
      (let [^LogicalTypeAnnotation$TimestampLogicalTypeAnnotation t ann]
        (partial convert/epoch->time (.isAdjustedToUTC t) (units-per-second (.getUnit t))))

      identity)))
