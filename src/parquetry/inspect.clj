(ns parquetry.inspect
  "Inspect a parquet artifact without trusting it.

  A key-shape mismatch once produced a 6 KB file whose footer correctly claimed
  174,128 rows while every column held nil. The footer was right, so it alone
  could not catch it: `inspect` reports the footer count AND the per-column fill.

  Attaches `nav` rather than importing a viewer, so Portal, REBL, Morse and
  plain `tap>` all work and the choice stays with the caller."
  (:require
    [clojure.core.protocols :as p]
    [parquetry.core :as pq]
    [parquetry.floor :as floor])
  (:import
    (org.apache.parquet.hadoop.metadata
      BlockMetaData)
    (org.apache.parquet.schema
      Type)))


(defn footer
  "Row count, row-group count and column names from the file FOOTER. ACTION.

  Reads no row data, so it is safe on an artifact too large to hold. On its
  own it is not evidence the file contains anything -- see the ns docstring."
  [path]
  (let [md     (floor/metadata path)
        blocks (.getBlocks md)]
    {:path       (str path)
     :row-count  (transduce (map (fn [^BlockMetaData b] (.getRowCount b))) + 0 blocks)
     :row-groups (count blocks)
     :columns    (mapv (fn [^Type f] (keyword (.getName f)))
                       (.getFields (.getSchema (.getFileMetaData md))))}))


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
  "Columns whose every value is nil, sorted. CALCULATION.

  Non-empty alongside a positive footer count is the incident signature
  described in the ns docstring."
  [summary]
  (->> summary
       (filter (fn [[_ {:keys [non-nil]}]] (zero? non-nil)))
       (map key)
       sort
       vec))


(defn verdict
  "One keyword judging an artifact from its footer row count, its all-null
  columns and the number of rows actually read. CALCULATION."
  [footer-row-count nulls read-row-count]
  (cond
    (zero? footer-row-count)                 :empty
    (seq nulls)                              :suspect-all-null-columns
    (not= footer-row-count read-row-count)   :footer-disagrees-with-rows
    :else                                    :ok))


(defn inspect
  "Footer + per-column fill + a sample, as a datafiable map. ACTION.

  Reads the whole file; prefer `footer` on a large one. `:rows` is a
  placeholder -- `nav` into it to realise them."
  [path]
  (let [ftr   (footer path)
        rows  (pq/read path)
        summ  (summary (:columns ftr) rows)
        nulls (all-null-columns summ)]
    (with-meta
      {:footer           ftr
       :summary          summ
       :all-null-columns nulls
       :verdict          (verdict (:row-count ftr) nulls (count rows))
       :sample           (vec (take 5 rows))
       :rows             (str (count rows) " rows -- nav here to expand")}
      {`p/nav (fn [_ k v] (if (= k :rows) rows v))})))


(defn tap!
  "`inspect` the artifact and `tap>` it. ACTION. Returns the map too."
  [path]
  (doto (inspect path) tap>))
