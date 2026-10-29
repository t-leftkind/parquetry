(ns parquetry.core
  "Write and read parquet files as Clojure maps.

  Callers work in maps with keyword keys, `BigDecimal`s, and `Instant`s (or
  `LocalDateTime`s, for a `:utc? false` timestamp column). A schema is a vector
  of columns; see `parquetry.schema` for the format. Nothing here exposes a
  `MessageType`, an unscaled long or a parquet-floor callback.

  Errors are ex-info with :type ::unsupported-column-type, ::inexact-decimal or
  ::missing-required."
  (:refer-clojure :exclude [read])
  (:require
    [parquetry.codec :as codec]
    [parquetry.convert :as convert]
    [parquetry.floor :as floor]
    [parquetry.schema :as schema]))


;; Kept from before the namespace split so existing callers of `pq/...` don't break.
(def message-type schema/message-type)
(def decimal->unscaled convert/decimal->unscaled)
(def unscaled->decimal convert/unscaled->decimal)
(def instant->micros convert/instant->micros)
(def micros->instant convert/micros->instant)
(def local-date-time->micros convert/local-date-time->micros)


(defn- missing-required
  [k row]
  (ex-info (str "parquet write: required column " k " is missing or nil")
           {:type     ::missing-required
            :column   k
            :row-keys (vec (sort (map str (keys row))))}))


(defn- column-writer
  [round-decimals? {col-key :name :keys [null?] :as column}]
  {:col-key  col-key
   :col-name (name col-key)
   :null?    null?
   :encode   (codec/encoder column round-decimals?)})


(defn- row->entries
  "Row -> the [column-name encoded-value] pairs to write, nils dropped. Throws on
  a required column with no value, since skipping it writes a valid but unreadable
  file; a key-shape mismatch otherwise surfaces a step later, blaming the reader."
  [writers row]
  (->> writers
       (keep (fn [{:keys [col-key col-name null? encode]}]
               (let [v (get row col-key)]
                 (cond
                   (some? v) [col-name (encode v)]
                   null?     nil
                   :else     (throw (missing-required col-key row))))))
       vec))


;; The fifth parameter is already the options map metrics/parameter-count asks for.
#_{:splint/disable [metrics/parameter-count]}


(defn write!
  "Write `rows` (maps with keyword keys; any reducible) to `path`. Returns the
  row count.

  Decimals are rounded HALF_EVEN to the column's scale unless
  `:round-decimals? false`, which throws {:type ::inexact-decimal} instead. A
  missing required value throws {:type ::missing-required}."
  ([path schema root rows] (write! path schema root rows {}))
  ([path schema root rows {:keys [round-decimals?] :or {round-decimals? true}}]
   (floor/write-file! (schema/message-type schema root)
                      path
                      (partial row->entries (mapv (partial column-writer round-decimals?) schema))
                      rows)))


(defn rows
  "A reducible over the rows of `path`, each a map as `read` returns, decoded one at a time.

  Every `reduce` (or `into`, `transduce`, `run!`) opens the file and closes it when it
  ends, early by `reduced` included; nothing is read until then. Use it when the rows
  would not fit in memory at once."
  [path]
  (reify clojure.lang.IReduceInit
    (reduce [_ f init] (floor/reduce-file path codec/decoder f init))))


(defn read
  "Read `path` into a vector of maps with keyword keys.

  Decoding is driven by the file's own schema, so decimals come back as
  `BigDecimal` and timestamps as `Instant` (or `LocalDateTime`, when the file
  says isAdjustedToUTC=false) without the caller supplying anything."
  [path]
  (vec (rows path)))


(defn file-schema
  "The MessageType recorded in the file, as a string. For contract assertions."
  [path]
  (str (floor/message-type path)))


(defn file-columns
  "The file's columns as a `write!` schema, read from the footer.

  Compares logically: two files with the same columns describe the same, whatever
  physical encoding each writer chose for a decimal. So a contract check is
  `(= expected-schema (file-columns path))`."
  [path]
  (mapv schema/field->column (.getFields (floor/message-type path))))
