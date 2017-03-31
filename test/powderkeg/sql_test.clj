(ns powderkeg.sql-test
  (:require [powderkeg.sql :as sql]
            [clojure.test :refer :all]
            [powderkeg.fixtures :refer [with-resources local-spark]]
            [clojure.spec :as s]))

(s/def ::name string?)
(s/def ::person (s/keys :req [::name]))

(deftest sql
  (let [in [{::name "Brian"} {::name "Brita"}]
        data-set (sql/df in ::person)]
    (is (= ["Brian" "Brita"]
           (map #(.getString % 0) (.collect data-set))))
    (is (= (s/form ::person)
           (s/form (sql/spec-of data-set))))
    (is (= in
           (into [] data-set)))
    (when (.startsWith (.version powderkeg.core/*sc*) "2.")
      (.createTempView data-set "people")
      (let [selection (sql/exec "select * from people")]
        (is (= (s/form ::person)
               (s/form (sql/spec-of selection))))))))
