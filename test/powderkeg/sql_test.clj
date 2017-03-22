(ns powderkeg.sql-test
  (:require [powderkeg.sql :as sql]
            [clojure.test :refer :all]
            [clojure.spec :as s]))

(s/def ::name string?)
(s/def ::person (s/keys :req [::name]))

(deftest sql
  (let [data-set (sql/df [{::name "Brian"} {::name "Brita"}] ::person)]
    (is (= ["Brian" "Brita"]
           (map #(.getString % 0) (.collect data-set))))
    (is (= (s/form ::person)
           (s/form (sql/spec-of data-set))))
    (when (.startsWith (.version powderkeg.core/*sc*) "2.")
      (.createTempView data-set "people")
      (let [selection (sql/exec "select * from people")]
        (is (= (s/form ::person)
               (s/form (sql/spec-of selection))))))))
