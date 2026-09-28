(ns parquetry.inspect
  "Inspect a parquet artifact without trusting it.

  From a real incident: a key-shape mismatch produced a 6 KB file whose
  footer correctly claimed 174,128 rows while every column held nil. The
  write reported success; it failed a step later, on read, blaming the
  reader. The footer alone could not have caught it -- the footer was right.

  So `inspect` reports the footer count AND the actual per-column fill.
  Either number alone is reassuring and wrong.

  Attaches `nav` rather than importing a viewer, so Portal, REBL, Morse and
  plain `tap>` all work and the choice stays with the caller."
  (:require [clojure.core.protocols :as p]
            [parquetry.core :as pq])
  (:import blue.strategic.parquet.ParquetReader
           java.io.File
           (org.apache.parquet.hadoop.metadata
             BlockMetaData
             ParquetMetadata)
           (org.apache.parquet.schema
             MessageType
             Type)))


;; ------------------------------------------------------------ actions

(defn footer
  "Row count, row-group count and column names from the file FOOTER. ACTION.

  Reads no row data, so it is safe on an artifact too large to hold. On its
  own it is not evidence the file contains anything -- see the ns docstring."
  [path]
  (let [^ParquetMetadata md (ParquetReader/readMetadata (File. (str path)))
        ^MessageType mt     (.getSchema (.getFileMetaData md))]
    {:path       (str path)
     :row-count  (reduce + 0 (map (fn [^BlockMetaData b] (.getRowCount b)) (.getBlocks md)))
     :row-groups (count (.getBlocks md))
     :columns    (mapv (fn [^Type f] (keyword (.getName f))) (.getFields mt))}))


;; ------------------------------------------------------- calculations

(defn summary
  "Per-column non-nil and nil counts. CALCULATION.

  Counts against `columns`, not the keys rows happen to carry, so a column
  absent from every row still appears with a zero count."
  [columns rows]
  (reduce (fn [acc row]
            (reduce (fn [acc col]
                      (update acc col update (if (some? (get row col)) :non-nil :nil) inc))
                    acc
                    columns))
          (zipmap columns (repeat {:non-nil 0 :nil 0}))
          rows))


(defn all-null-columns
  "Columns whose every value is nil. CALCULATION.

  Non-empty alongside a positive footer count is the incident signature
  described above."
  [summary]
  (->> summary
       (filter (fn [[_ {:keys [non-nil]}]] (zero? non-nil)))
       (mapv key)
       (sort)
       (vec)))


;; ------------------------------------------------------------ actions

(defn inspect
  "Footer + per-column fill + a sample, as a datafiable map. ACTION.

  Reads the whole file; prefer `footer` on a large one. `:rows` is a
  placeholder -- `nav` into it to realise them."
  [path]
  (let [ftr  (footer path)
        rows (pq/read path)
        summ (summary (:columns ftr) rows)
        nulls (all-null-columns summ)]
    (with-meta
      {:footer           ftr
       :summary          summ
       :all-null-columns nulls
       :verdict          (cond
                           (zero? (:row-count ftr))          :empty
                           (seq nulls)                       :suspect-all-null-columns
                           (not= (:row-count ftr) (count rows)) :footer-disagrees-with-rows
                           :else                             :ok)
       :sample           (vec (take 5 rows))
       :rows             (str (count rows) " rows -- nav here to expand")}
      {`p/nav (fn [_ k v] (if (= k :rows) rows v))})))


(defn tap!
  "`inspect` the artifact and `tap>` it. ACTION. Returns the map too."
  [path]
  (doto (inspect path) (tap>)))
