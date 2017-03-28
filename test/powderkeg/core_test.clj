(ns powderkeg.core-test
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]
            [powderkeg.fixtures :refer [with-resources local-spark]]
            [powderkeg.asserts :refer [example-asserts]]))

(deftest rdd
  (with-resources
    [local-spark]
    (example-asserts)))

(deftest pair-rdd
  (with-resources
    [local-spark]
    (is (instance? org.apache.spark.api.java.JavaRDD
                   (keg/rdd {:a 1 :b 2})))))

(deftest redef
  (with-resources
    [local-spark]
    (is (= (eval '(do (defn ++ [x] (inc x)) (into [] (powderkeg.core/rdd (range 5) (map ++)))))
          (map inc (range 5))))
    (is (= (eval '(do (defn ++ [x] (dec x)) (into [] (powderkeg.core/rdd (range 5) (map ++)))))
          (map dec (range 5))))))