(ns powderkeg.sql-test
  (:require [powderkeg.sql :as sql]
            [clojure.test :refer :all]
            [powderkeg.fixtures :refer [with-local]]
            [clojure.spec :as s]))

(use-fixtures :once with-local)

(s/def ::name string?)
(s/def ::person (s/keys :req [::name]))

(deftest sql
  (let [data-set (sql/df [{::name "Brian"}] ::person)]
    (is (= "Brian"
           (.getString (.first data-set) 0)))))
