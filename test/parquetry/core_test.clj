(ns parquetry.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [parquetry.core :as pq])
  (:import java.io.File
           (java.time Instant
                      LocalDateTime)))


(defn- tmp
  []
  (str (File/createTempFile "parquetry" ".parquet")))


(def ^:private fixture "test/fixtures/pyarrow.parquet")
(def ^:private ldt (LocalDateTime/of 2026 9 15 12 0 0 123456000))


;; ------------------------------------------------------------ timestamps

(deftest a-timestamp-is-utc-and-an-instant-by-default
  (let [path   (tmp)
        schema [{:name :at :type :timestamp}]
        at     (Instant/parse "2026-09-15T12:00:00.123456Z")]
    (pq/write! path schema "t" [{:at at}])

    (is (= [{:at at}] (pq/read path)))
    (is (= schema (mapv #(dissoc % :null?) (pq/file-columns path))))))


(deftest a-naive-timestamp-round-trips-as-a-local-date-time
  (let [path   (tmp)
        schema [{:name :at :type :timestamp :utc? false}]]
    (pq/write! path schema "t" [{:at ldt}])

    (is (= [{:at ldt}] (pq/read path)))
    (is (= schema (pq/file-columns path)))))


(deftest timestamps-in-every-unit-decode-from-the-files-annotation
  (let [[row] (pq/read fixture)]
    (is (= ldt (:naive_us row)))
    (is (= (LocalDateTime/of 2026 9 15 12 0 0 123000000) (:naive_ms row)))
    (is (= ldt (:naive_ns row)))
    (is (= (Instant/parse "2026-09-15T12:00:00.123Z") (:utc_ms row)))))


;; -------------------------------------------------------------- booleans

(deftest a-boolean-round-trips-required-and-optional
  (let [path   (tmp)
        schema [{:name :a :type :boolean}
                {:name :b :type :boolean :null? true}]
        rows   [{:a true :b false} {:a false :b nil} {:a true :b true}]]
    (pq/write! path schema "t" rows)

    (is (= rows (pq/read path)))
    (is (= schema (pq/file-columns path)))))


;; -------------------------------------------------------------- decimals

(deftest decimals-from-other-writers-decode-whatever-their-physical-type
  (is (= [[1.500M 2.50M] [-9999.999M 0.00M] [nil nil]]
         (mapv (juxt :small :large) (pq/read fixture)))))


(deftest a-decimal-is-rounded-half-even-by-default
  (let [path (tmp)]
    (pq/write! path [{:name :d :type :decimal :precision 7 :scale 3}] "t" [{:d 1.2345M}])

    (is (= [{:d 1.234M}] (pq/read path)))))


(deftest a-strict-write-refuses-a-decimal-that-would-round
  (let [e (try (pq/write! (tmp) [{:name :d :type :decimal :precision 7 :scale 3}] "t" [{:d 1.2345M}] {:round-decimals? false})
               nil
               (catch clojure.lang.ExceptionInfo e e))]
    (is (= ::pq/inexact-decimal (:type (ex-data e))))
    (is (= :d (:column (ex-data e))))))


(deftest a-decimal-is-int32-up-to-9-digits-and-int64-beyond
  (let [path   (tmp)
        schema [{:name :small :type :decimal :precision 9 :scale 3}
                {:name :large :type :decimal :precision 10 :scale 2}]]
    (pq/write! path schema "t" [{:small 123456.789M :large 12345678.90M}])

    (is (re-find #"required int32 small" (pq/file-schema path)))
    (is (re-find #"required int64 large" (pq/file-schema path)))
    (is (= [{:small 123456.789M :large 12345678.90M}] (pq/read path)))))


(deftest a-decimal-wider-than-int64-is-refused-up-front
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"precision 19"
                        (pq/message-type [{:name :d :type :decimal :precision 19 :scale 2}] "t"))))


;; --------------------------------------------------------------- schema

(deftest file-columns-describe-a-file-in-write-schema-terms
  (testing "parquetry's own file reads back as the schema it was written with"
    (let [path   (tmp)
          schema [{:name :s :type :string}
                  {:name :n :type :long :null? true}
                  {:name :d :type :decimal :precision 7 :scale 3}
                  {:name :at :type :timestamp :utc? false}]]
      (pq/write! path schema "t" [])

      (is (= schema (pq/file-columns path)))))

  (testing "another writer's file, physical encoding aside"
    (is (= [{:name :small :type :decimal :precision 7 :scale 3 :null? true}
            {:name :large :type :decimal :precision 18 :scale 2 :null? true}
            {:name :naive_us :type :timestamp :utc? false :null? true}
            {:name :naive_ms :type :timestamp :utc? false :unit :millis :null? true}
            {:name :naive_ns :type :timestamp :utc? false :unit :nanos :null? true}
            {:name :utc_ms :type :timestamp :unit :millis :null? true}]
           (pq/file-columns fixture)))))


(deftest a-missing-required-value-is-a-typed-error
  (let [e (try (pq/write! (tmp) [{:name :s :type :string}] "t" [{}]) nil (catch clojure.lang.ExceptionInfo e e))]
    (is (= ::pq/missing-required (:type (ex-data e))))
    (is (= :s (:column (ex-data e))))))
