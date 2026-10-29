(ns parquetry.inspect-test
  (:require
    [clojure.datafy :as datafy]
    [clojure.test :refer [deftest is]]
    [matcher-combinators.test]
    [parquetry.core :as pq]
    [parquetry.inspect :as inspect]
    [parquetry.test-util :refer [tmp-path]]))


(def ^:private descriptors
  [{:name :id   :type :long}
   {:name :name :type :string}
   {:name :note :type :string :null? true}])


(defn- write-tmp!
  [rows]
  (let [path (tmp-path)]
    (pq/write! path descriptors "t" rows)
    path))


(deftest footer-reports-the-row-count-without-reading-rows
  (let [path (write-tmp! [{:id 1 :name "a" :note "x"} {:id 2 :name "b" :note nil}])
        f    (inspect/footer path)]
    (is (= 2 (:row-count f)))
    (is (= [:id :name :note] (:columns f)))
    (is (pos? (:row-groups f)))))


(deftest summary-counts-nils-per-column
  (is (= {:id   {:non-nil 2 :nil 0}
          :note {:non-nil 1 :nil 1}}
         (inspect/summary [:id :note]
                          [{:id 1 :note "x"} {:id 2 :note nil}]))))


(deftest summary-counts-a-column-absent-from-every-row
  ;; The incident case: the writer looked up keys that no row carried, so the
  ;; column is not merely nil -- it is missing. It must still be counted, or
  ;; the very signal worth seeing is the one that disappears.
  (is (= {:ghost {:non-nil 0 :nil 2}}
         (inspect/summary [:ghost] [{:id 1} {:id 2}]))))


(deftest all-null-columns-names-them
  (is (= [:a :b]
         (inspect/all-null-columns {:a {:non-nil 0 :nil 3}
                                    :b {:non-nil 0 :nil 3}
                                    :c {:non-nil 3 :nil 0}}))))


(deftest a-healthy-artifact-reads-ok
  (let [path (write-tmp! [{:id 1 :name "a" :note "x"}])
        r    (inspect/inspect path)]
    (is (match? {:verdict :ok :all-null-columns []} r))))


(deftest the-finding-9-signature-is-caught
  ;; A file whose footer row count is CORRECT while every optional column is
  ;; empty. Previously indistinguishable from a healthy file until a later
  ;; step failed on read, blaming the reader.
  (let [path (write-tmp! [{:id 1 :name "a" :note nil}
                          {:id 2 :name "b" :note nil}])
        r    (inspect/inspect path)]
    (is (match? {:footer           {:row-count 2}
                 :all-null-columns [:note]
                 :verdict          :suspect-all-null-columns}
                r)
        "the footer row count is right, which is the trap")))


(deftest an-empty-artifact-is-reported-as-empty-not-suspect
  (let [path (write-tmp! [])]
    (is (= :empty (:verdict (inspect/inspect path))))))


(deftest rows-are-behind-nav-so-the-initial-view-stays-small
  (let [path (write-tmp! [{:id 1 :name "a" :note "x"}])
        r    (inspect/inspect path)]
    (is (string? (:rows r)) "the initial value is a placeholder, not the rows")
    (is (= [{:id 1 :name "a" :note "x"}]
           (datafy/nav r :rows (:rows r)))
        "nav expands it")))
