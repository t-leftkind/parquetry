(ns parquetry.test-util
  (:import
    java.io.File))


(defn tmp-path
  "A fresh temp file path, deleted when the JVM exits."
  []
  (str (doto (File/createTempFile "parquetry" ".parquet")
         .deleteOnExit)))
