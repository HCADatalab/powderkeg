(ns powderkeg.core-test
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]))

(defn with-local [f]
  (keg/connect! "local[2]")
  (f)
  (keg/disconnect!))

(use-fixtures :once with-local)

(deftest rdd
  (is (into [] (keg/rdd (range 10)))
      (into [] (range 10))))
