(ns parquetry.floor
  "parquet-floor interop. ACTIONS. The rest of parquetry speaks Clojure data.

  Three parquet-floor gotchas live here so nothing else has to know them:

    1. `Dehydrator/dehydrate` returns void, so its `reify` body must end in nil.
    2. `Hydrator/add` receives a String[] column PATH, not a String; `(str k)`
       yields \"[Ljava.lang.String;@...\" and every lookup silently misses.
    3. Nulls must be skipped on write, not written as nil."
  (:import
    (blue.strategic.parquet
      Dehydrator
      Hydrator
      ParquetReader
      ParquetWriter
      ValueWriter)
    java.io.File
    java.util.stream.Stream
    (org.apache.parquet.hadoop.metadata
      ParquetMetadata)
    (org.apache.parquet.schema
      MessageType
      PrimitiveType)))


(defn metadata
  "The footer of the file at `path`. Reads no row data."
  ^ParquetMetadata [path]
  (ParquetReader/readMetadata (File. (str path))))


(defn message-type
  "The MessageType recorded in the footer of the file at `path`."
  ^MessageType [path]
  (.getSchema (.getFileMetaData (metadata path))))


(defn- dehydrator
  [row->entries]
  (reify Dehydrator
    (dehydrate
      [_ row vw]
      (doseq [[col v] (row->entries row)]
        (.write ^ValueWriter vw ^String col v))
      nil)))


(defn write-file!
  "Write `rows` to `path` as `mt`, reducing them so any IReduceInit works.
  `row->entries` turns a row into [column-name value] pairs, nils already
  dropped. Returns the row count."
  [^MessageType mt path row->entries rows]
  (with-open [w (ParquetWriter/writeFile mt (File. (str path)) (dehydrator row->entries))]
    (reduce (fn [n row]
              (.write w row)
              (inc n))
            0
            rows)))


(defn- hydrator
  ^Hydrator [decoders]
  (reify Hydrator
    (start [_] (transient {}))

    (add
      [_ acc k v]
      (let [col (last k)]
        ;; parquet-floor passes nil for an absent optional value; decoding it NPEs.
        (assoc! acc (keyword col) (when (some? v) ((decoders col) v)))))

    (finish [_ acc] (persistent! acc))))


(defn reduce-file
  "Reduce `f` over the rows of `path`, each a map with keyword keys, values
  decoded by `(decoder field)`. Closes the file on finish and on `reduced`."
  [path decoder f init]
  (let [decoders (->> (.getFields (message-type path))
                      (map (fn [^PrimitiveType field] [(.getName field) (decoder field)]))
                      (into {}))]
    (with-open [^Stream s (ParquetReader/streamContent (File. (str path)) (hydrator decoders))]
      (stream-reduce! f init s))))
