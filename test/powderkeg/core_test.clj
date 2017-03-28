(ns powderkeg.core-test
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]))

(defn clojure-dynamic-classloader [f]
  (let [cl (.getContextClassLoader (Thread/currentThread))]
    (.setContextClassLoader (Thread/currentThread) (clojure.lang.DynamicClassLoader. cl))
    (try
      (f)
      (finally 
        (.setContextClassLoader (Thread/currentThread) cl)))))

(defn with-local [f]
  (keg/connect! "local[2]")
  (try
    (f)
    (finally
      (keg/disconnect!))))

(use-fixtures :once clojure-dynamic-classloader with-local)

(deftest rdd
  (is (= (into [] (keg/rdd (range 10)))
         (into [] (range 10))))
  (is (= ["Testing spark"]
         (into [] (keg/rdd ["This is a firest line"
                            "Testing spark"
                            "and powderkeg"
                            "Happy hacking!"]
                           (filter #(.contains % "spark"))))))
  (is (instance? org.apache.spark.api.java.JavaRDD
                 (keg/rdd (range 100)      ; source
                          (filter odd?)    ; 1st transducer to apply
                          (map inc)        ; 2nd transducer
                          :partitions 2))) ; and options
  (is (= [0 1 2 3 4 5 6 7 8 9]
         (into [] (keg/rdd (range 10)))))
  (is (= [0 1 2 3 4]
         (into [] (keg/scomp (take 5)) (range 10))))
  (is (= [0 1 2 3 4]
         (keg/into [] (take 5) (range 10))))
  (is (= [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17]]
         (into [] (partition-by #(quot % 6)) (keg/rdd (range 20)))))
  (is (= [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17] [18 19]]
         (keg/into [] (partition-by #(quot % 6)) (keg/rdd (range 20)))))
  (is (= [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17] [18 19]]
         (into [] (keg/scomp (partition-by #(quot % 6))) (keg/rdd (range 20)))))
  (is (= {:a [[1 11] "aa"] :c ["x" "cc"] :b [2 "y"]}
         (into {} (keg/join (keg/rdd {:a [1 11] :b 2}) :or "x" (keg/rdd {:a "aa" :c "cc"} ) :or "y")))))


(deftest pair-rdd
  (is (instance? org.apache.spark.api.java.JavaRDD
                 (keg/rdd {:a 1 :b 2}))))

(deftest redef
  (is (= (eval '(do (defn ++ [x] (inc x)) (into [] (powderkeg.core/rdd (range 5) (map ++)))))
        (map inc (range 5))))
  (is (= (eval '(do (defn ++ [x] (dec x)) (into [] (powderkeg.core/rdd (range 5) (map ++)))))
        (map dec (range 5)))))
