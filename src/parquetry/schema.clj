(ns parquetry.schema
  "Columns <-> parquet fields, in both directions. CALCULATIONS.

  A schema is a vector of columns:

    [{:name :tier          :type :string}
     {:name :total_revenue :type :decimal :precision 18 :scale 6 :null? true}
     {:name :created_at    :type :timestamp}
     {:name :pg_created_at :type :timestamp :utc? false}]

  `:null?` defaults to false (required). `:utc?` defaults to true: the column
  holds `Instant`s (isAdjustedToUTC=true); `:utc? false` is a naive wall-clock
  timestamp holding `LocalDateTime`s, as Postgres `timestamp` stores it.

  Errors are typed `:parquetry.core/...` because that is what callers match on.
  A malformed column (no name, a decimal without precision or scale) is a
  programmer error instead: a Truss assertion whose `:data` holds the column."
  (:require
    [taoensso.truss :refer [have]])
  (:import
    (org.apache.parquet.schema
      LogicalTypeAnnotation
      LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
      LogicalTypeAnnotation$StringLogicalTypeAnnotation
      LogicalTypeAnnotation$TimeUnit
      LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
      MessageType
      PrimitiveType
      PrimitiveType$PrimitiveTypeName
      Type$Repetition
      Types)))


(def ^:private max-int32-precision 9)
(def ^:private max-int64-precision 18)


(defn int32-decimal?
  "Whether a decimal column is stored as INT32 (up to 9 digits) rather than
  INT64, as the parquet spec recommends; parquet-java warns otherwise."
  [{:keys [precision]}]
  (<= precision max-int32-precision))


(defn- builder
  [primitive null?]
  (if null?
    (Types/optional primitive)
    (Types/required primitive)))


(defn- validate-decimal
  [{col-name :name :keys [precision scale] :as column}]
  (have pos-int? precision :data {:column column})
  (have nat-int? scale :data {:column column})
  (when (> precision max-int64-precision)
    (throw (ex-info (str "decimal column " col-name " has precision " precision
                         "; INT64 holds at most " max-int64-precision)
                    {:type      :parquetry.core/unsupported-column-type
                     :column    col-name
                     :precision precision}))))


(defn- column->field
  [{col-name :name col-type :type :keys [null? precision scale utc?] :or {utc? true} :as column}]
  (let [col (name (have [:or keyword? string?] col-name :data {:column column}))]
    (case col-type
      :string
      (-> (builder PrimitiveType$PrimitiveTypeName/BINARY null?)
          (.as (LogicalTypeAnnotation/stringType))
          (.named col))

      :long
      (.named (builder PrimitiveType$PrimitiveTypeName/INT64 null?) col)

      :boolean
      (.named (builder PrimitiveType$PrimitiveTypeName/BOOLEAN null?) col)

      :decimal
      (do (validate-decimal column)
          (-> (builder (if (int32-decimal? column)
                         PrimitiveType$PrimitiveTypeName/INT32
                         PrimitiveType$PrimitiveTypeName/INT64)
                       null?)
              ;; decimalType takes (scale, precision), the reverse of DECIMAL(p, s).
              (.as (LogicalTypeAnnotation/decimalType (int scale) (int precision)))
              (.named col)))

      :timestamp
      (-> (builder PrimitiveType$PrimitiveTypeName/INT64 null?)
          (.as (LogicalTypeAnnotation/timestampType (boolean utc?) LogicalTypeAnnotation$TimeUnit/MICROS))
          (.named col))

      (throw (ex-info "unsupported parquet column type"
                      {:type   :parquetry.core/unsupported-column-type
                       :column col-name
                       :given  col-type})))))


(defn message-type
  "Schema -> the MessageType named `root`. Throws
  {:type :parquetry.core/unsupported-column-type} for a column it cannot store."
  ^MessageType [schema root]
  (let [b (reduce (fn [b column] (.addField b (column->field column)))
                  (Types/buildMessage)
                  schema)]
    (.named b root)))


(def ^:private unit-names
  {LogicalTypeAnnotation$TimeUnit/MILLIS :millis
   LogicalTypeAnnotation$TimeUnit/NANOS  :nanos})


(defn- annotated-type
  [ann]
  (condp instance? ann
    LogicalTypeAnnotation$StringLogicalTypeAnnotation
    {:type :string}

    LogicalTypeAnnotation$DecimalLogicalTypeAnnotation
    (let [^LogicalTypeAnnotation$DecimalLogicalTypeAnnotation d ann]
      {:type      :decimal
       :precision (.getPrecision d)
       :scale     (.getScale d)})

    LogicalTypeAnnotation$TimestampLogicalTypeAnnotation
    (let [^LogicalTypeAnnotation$TimestampLogicalTypeAnnotation t ann
          unit (unit-names (.getUnit t))]
      (cond-> {:type :timestamp}
        (not (.isAdjustedToUTC t)) (assoc :utc? false)
        unit                       (assoc :unit unit)))

    nil))


(defn- unannotated-type
  [^PrimitiveType field]
  (condp = (.getPrimitiveTypeName field)
    PrimitiveType$PrimitiveTypeName/INT64   {:type :long}
    PrimitiveType$PrimitiveTypeName/BOOLEAN {:type :boolean}
    nil))


(defn field->column
  "A parquet field -> a column. Defaults are left out, as in a hand-written
  schema: :null? appears only when true, :utc? only when false, :unit only when
  not micros. A field with no column equivalent is {:type :unknown :parquet \"...\"}."
  [^PrimitiveType field]
  (let [ann (.getLogicalTypeAnnotation field)]
    (cond-> (merge {:name (keyword (.getName field))}
                   (or (if ann (annotated-type ann) (unannotated-type field))
                       {:type :unknown :parquet (str field)}))
      (= Type$Repetition/OPTIONAL (.getRepetition field)) (assoc :null? true))))
