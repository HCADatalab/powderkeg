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
             (.getString (.first data-set) 0))))))
