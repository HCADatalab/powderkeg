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
