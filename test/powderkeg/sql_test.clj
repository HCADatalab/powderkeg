(ns powderkeg.sql-test
  (:require [powderkeg.sql :as sql]
            [clojure.test :refer :all]
            [powderkeg.fixtures :refer [with-resources local-spark]]
            [clojure.spec :as s]))

(s/def ::name string?)
(s/def ::person (s/keys :req [::name]))

(deftest sql
  (with-resources
    [local-spark]
    (let [data-set (sql/df [{::name "Brian"}] ::person)]
      (is (= "Brian"
             (.getString (.first data-set) 0)))
      (is (= (s/form ::person)
             (s/form (sql/spec-of data-set))))
      (.createTempView data-set "people")
      (let [selection (sql/exec "select * from people")]
        (is (= (s/form ::person)
               (s/form (sql/spec-of selection))))))))
