(ns parquetry.convert
  "Conversions between Clojure values and the numbers parquet stores. CALCULATIONS.

  JDK classes only, so `parquetry.malli` can share them without loading parquet."
  (:require
    [clojure.math :as math])
  (:import
    (java.math
      BigDecimal
      BigInteger
      RoundingMode)
    (java.time
      Instant
      LocalDateTime
      ZoneOffset)))


(def ^:private micros-per-second 1000000)
(def ^:private nanos-per-second 1000000000)


(defn decimal->unscaled
  "A decimal -> the unscaled long parquet stores. HALF_EVEN by default;
  RoundingMode/UNNECESSARY throws ArithmeticException rather than round."
  (^long [v scale] (decimal->unscaled v scale RoundingMode/HALF_EVEN))
  (^long [v scale ^RoundingMode mode]
   (-> (bigdec v)
       (.setScale (int scale) mode)
       (.unscaledValue)
       (.longValueExact))))


(defn unscaled->decimal
  "The unscaled long parquet stores -> BigDecimal."
  ^BigDecimal [raw scale]
  (BigDecimal. (BigInteger/valueOf (long raw)) (int scale)))


(defn instant->micros
  "Instant -> epoch microseconds, the physical form of a TIMESTAMP(MICROS)
  column. Passes a long straight through so callers may supply either."
  ^long [v]
  (if (instance? Instant v)
    (+ (* (.getEpochSecond ^Instant v) micros-per-second)
       (quot (.getNano ^Instant v) 1000))
    (long v)))


(defn local-date-time->micros
  "LocalDateTime -> microseconds since the epoch, read as wall-clock time, the
  physical form of a naive TIMESTAMP(MICROS). Passes a long straight through."
  ^long [v]
  (if (instance? LocalDateTime v)
    (+ (* (.toEpochSecond ^LocalDateTime v ZoneOffset/UTC) micros-per-second)
       (quot (.getNano ^LocalDateTime v) 1000))
    (long v)))


(defn epoch->time
  "`v` units since the epoch, at `per-second` units a second -> Instant, or
  LocalDateTime when not `utc?`."
  [utc? per-second v]
  (let [per   (long per-second)
        secs  (math/floor-div (long v) per)
        nanos (* (math/floor-mod (long v) per) (quot nanos-per-second per))]
    (if utc?
      (Instant/ofEpochSecond secs nanos)
      (LocalDateTime/ofEpochSecond secs (int nanos) ZoneOffset/UTC))))


(defn micros->instant
  "Epoch microseconds -> Instant."
  ^Instant [v]
  (epoch->time true micros-per-second v))


(defn micros->local-date-time
  "Microseconds since the epoch -> LocalDateTime, read as wall-clock time."
  ^LocalDateTime [v]
  (epoch->time false micros-per-second v))
