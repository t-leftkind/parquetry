(ns parquetry.malli
  "malli schemas as the single source for a dataset's shape.

  Declare a dataset once as a malli `:map`; derive the parquet column
  descriptors, validation and test data from it. Without this the shape gets
  stated twice -- once in whatever contract the project keeps, once as a
  hand-written descriptor vector -- linked only by a docstring.

  `parquetry.core` does not depend on this namespace and this one does not
  depend on it: a consumer writing its own descriptors pays nothing for the
  bridge existing."
  (:require [malli.core :as m])
  (:import (java.math
             BigDecimal
             BigInteger)
           java.time.Instant))


;; ------------------------------------------------------------ data
;;
;; Column types parquet can store are carried in malli PROPERTIES under the
;; :parquet/* namespace rather than inferred from predicates. A predicate
;; says what a value IS; it cannot say what precision to store it at.

(def instant
  "A TIMESTAMP(MICROS) column.

  The generator emits microsecond precision deliberately: finer values would
  fail a round-trip test on the encoding rather than on anything useful."
  [:fn {:parquet/type :timestamp
        :gen/schema   [:int {:min 0 :max 4102444800000000}]  ; epoch micros, .. year 2100
        :gen/fmap     (fn [micros]
                        (Instant/ofEpochSecond (quot micros 1000000)
                                               (* 1000 (rem micros 1000000))))}
   #(instance? Instant %)])


(defn decimal
  "A DECIMAL(precision, scale) column.

  Precision and scale stated once, so a column cannot declare 18,6 and be
  encoded at another scale. The generator covers the full unscaled range
  including negatives -- these exist to stress the rendering contract."
  [precision scale]
  (let [bound (-> (BigInteger/valueOf 10)
                  (.pow precision)
                  (.subtract BigInteger/ONE)
                  (.longValueExact))]
    [:fn {:parquet/type      :decimal
          :parquet/precision precision
          :parquet/scale     scale
          :gen/schema        [:int {:min (- bound) :max bound}]
          :gen/fmap          (fn [unscaled]
                               (BigDecimal. (BigInteger/valueOf unscaled) (int scale)))}
     #(instance? BigDecimal %)]))


;; ------------------------------------------------------- calculations

(def ^:private malli-type->parquet-type
  "malli types needing no :parquet/type property, because the mapping is
  unambiguous. Anything else must declare its parquet type explicitly."
  {:string :string
   :int    :long})


(defn- unsupported
  [msg schema]
  (throw (ex-info msg {:type ::unsupported-schema :schema (m/form schema)})))


(defn- column-type
  "One non-nullable malli schema -> the type part of a column descriptor.

  CALCULATION. Throws rather than guessing: half-supporting a type would
  surface later as an unreadable file."
  [s]
  (let [props (m/properties s)]
    (if-let [t (:parquet/type props)]
      (cond-> {:type t}
        (= :decimal t) (assoc :precision (:parquet/precision props)
                              :scale     (:parquet/scale props)))
      (if-let [t (malli-type->parquet-type (m/type s))]
        {:type t}
        (unsupported (str "no parquet column type for malli schema: " (pr-str (m/form s))) s)))))


(defn ->column-descriptors
  "A dataset's malli `:map` schema -> the descriptor vector
  `parquetry.core/write!` consumes. CALCULATION.

  Column order follows declaration order -- that is the physical order in
  the file. `[:maybe X]` becomes `:null? true`; everything else is required.

  Throws `{:type ::unsupported-schema}` at namespace load, since call sites
  are `def`s."
  [schema]
  (let [s (m/schema schema)]
    (when-not (= :map (m/type s))
      (unsupported "a dataset schema must be a :map" s))
    (mapv (fn [[k _entry-props child]]
            (let [nullable? (= :maybe (m/type child))
                  inner     (if nullable? (first (m/children child)) child)]
              (cond-> (assoc (column-type inner) :name k)
                nullable? (assoc :null? true))))
          (m/children s))))
