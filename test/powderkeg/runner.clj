(ns powderkeg.runner
  (:require [clojure.test :as t]
            [powderkeg.fixtures :refer [with-resources local-spark clojure-dynamic-classloader keg-connection]]
            [powderkeg.docker :refer [spark]]))

(defn load-test-namespaces []
  (run! require (eduction (comp (map #(.getName %))
                                (filter #(.endsWith % "test.clj"))
                                (map #(as-> % $
                                        (.replaceAll $ "_" "-")
                                        (.replaceAll $ "\\.clj$" "")
                                        (str "powderkeg." $)
                                        (symbol $))))
                          (file-seq (java.io.File. "test")))))

(load-test-namespaces)

(defn run-all-tests []
  (let [{:keys [error fail]} (t/run-all-tests #"^powderkeg\..*-test$")]
    (when-not (zero? (+ error fail))
      (throw (Exception. (str "Tests failed: " error " errors, " fail " failures"))))))

(defn run-tests-local-spark []
  (with-resources
    [clojure-dynamic-classloader
     local-spark]
    (run-all-tests)))

(defn run-tests-foreign-spark-1.5 []
  (with-resources
    [(spark "1.5.2-hadoop-2.6")
     clojure-dynamic-classloader
     (keg-connection "master")]
    (run-all-tests)))

(defn run-tests-foreign-spark-2.1 []
  (with-resources
    [(spark "2.1.0-hadoop-2.7")
     clojure-dynamic-classloader
     (keg-connection "localhost")]
    (run-all-tests)))
