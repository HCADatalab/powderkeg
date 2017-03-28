(ns powderkeg.core-test
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]
            [powderkeg.asserts :refer [example-asserts]]))

(deftest rdd
  (example-asserts))

(deftest pair-rdd
  (is (instance? org.apache.spark.api.java.JavaRDD
                 (keg/rdd {:a 1 :b 2}))))

(deftest redef
  (is (= (eval '(do (defn ++ [x] (inc x)) (into [] (powderkeg.core/rdd (range 5) (map ++)))))
         (map inc (range 5))))
  (is (= (eval '(do (defn ++ [x] (dec x)) (into [] (powderkeg.core/rdd (range 5) (map ++)))))
         (map dec (range 5)))))
