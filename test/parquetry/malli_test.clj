(ns parquetry.malli-test
  (:require [clojure.test :refer [deftest is testing]]
            [malli.generator :as mg]
            [parquetry.core :as pq]
            [parquetry.malli :as pqm])
  (:import java.io.File
           java.math.BigDecimal
           java.time.Instant))


;; ------------------------------------------------ ->column-descriptors

(deftest maps-each-supported-malli-type-to-a-parquet-column
  (is (= [{:type :string :name :a}
          {:type :long   :name :b}]
         (pqm/->column-descriptors [:map [:a :string] [:b :int]]))))


(deftest maybe-becomes-an-optional-column
  (is (= [{:type :string :name :a :null? true}]
         (pqm/->column-descriptors [:map [:a [:maybe :string]]]))
      "required is the descriptor format's default, so :null? appears only when true"))


(deftest carries-precision-and-scale-off-the-decimal-schema
  (is (= [{:type :decimal :precision 18 :scale 6 :name :amount}]
         (pqm/->column-descriptors [:map [:amount (pqm/decimal 18 6)]]))))


(deftest instant-becomes-a-timestamp-column
  (is (= [{:type :timestamp :name :at}]
         (pqm/->column-descriptors [:map [:at pqm/instant]]))))


(deftest declaration-order-is-the-column-order
  ;; Physical column order in the file follows this, so it is load-bearing
  ;; rather than cosmetic.
  (is (= [:c :a :b]
         (mapv :name (pqm/->column-descriptors
                       [:map [:c :string] [:a :string] [:b :string]])))))


(deftest an-unmappable-type-throws-rather-than-guessing
  (try
    (pqm/->column-descriptors [:map [:a :boolean]])
    (is false "expected ex-info")
    (catch clojure.lang.ExceptionInfo e
      (is (= :parquetry.malli/unsupported-schema (:type (ex-data e))))
      (is (re-find #":boolean" (ex-message e))
          "the message must name the offending schema, not just fail"))))


(deftest a-non-map-schema-is-rejected
  (try
    (pqm/->column-descriptors [:vector :string])
    (is false "expected ex-info")
    (catch clojure.lang.ExceptionInfo e
      (is (= :parquetry.malli/unsupported-schema (:type (ex-data e)))))))


;; ---------------------------------------------- generative round trip

(def ^:private Sale
  [:map {:closed true}
   [:id     :int]
   [:sku    :string]
   [:note   [:maybe :string]]
   [:at     pqm/instant]
   [:amount (pqm/decimal 18 6)]
   [:fee    [:maybe (pqm/decimal 18 6)]]])


(defn- tmp-path
  []
  (str (File/createTempFile "parquetry-gen" ".parquet")))


(deftest generated-rows-survive-a-parquet-round-trip
  ;; The reason the schemas carry generators. Hand-written fixtures test the
  ;; values someone thought of; this covers the full DECIMAL(18,6) unscaled
  ;; range including negatives, and micro-precision instants across ~130
  ;; years -- the space where encoding bugs actually live.
  (let [descriptors (pqm/->column-descriptors Sale)
        rows        (mg/sample Sale {:size 200})
        path        (tmp-path)]
    (is (pos? (count rows)))
    (pq/write! path descriptors "sale" rows)
    (let [back (pq/read path)]
      (is (= (count rows) (count back)))
      (is (= (vec rows) back)
          "every generated value must round-trip exactly, nils included"))))


(deftest generated-decimals-keep-their-scale-exactly
  (testing "scale survives, so a value never silently re-rounds"
    (let [vals (mg/sample (pqm/decimal 18 6) {:size 100})]
      (is (every? #(instance? BigDecimal %) vals))
      (is (every? #(= 6 (.scale ^BigDecimal %)) vals)))))


(deftest generated-instants-are-microsecond-precision
  ;; A nanosecond-precision generator would fail the round-trip test above on
  ;; the encoding rather than on anything worth knowing, since the column is
  ;; TIMESTAMP(MICROS).
  (let [vals (mg/sample pqm/instant {:size 100})]
    (is (every? #(instance? Instant %) vals))
    (is (every? #(zero? (rem (.getNano ^Instant %) 1000)) vals))))
