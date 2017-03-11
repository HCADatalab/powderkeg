(ns powderkeg.integration-test
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]))

(defn sh! [& args]
  (let [{:keys [exit out] :as ret} (apply sh args)]
    (when-not (zero? exit)
      (throw (Exception. (str "Problem while running '" (s/join " " args) "': " out))))
    out))

(defn start-master [pwd]
  (sh! "docker" "run" "-d"
       "--name" "master"
       "-h" "master"
       "-p" "8080:8080"
       "-p" "7077:7077"
       "-p" "4040:4040"
       "-p" "6066:6066"
       "--expose" "7001-7006"
       "--expose" "7077"
       "--expose" "6066"
       "-e" "MASTER=spark://master:7077"
       "-e" "SPARK_CONF_DIR=/conf"
       "-v" (str pwd "/conf/master:/conf")
       "-v" (str pwd "/data:/tmp/data")
       "gettyimages/spark"
       "bin/spark-class"
       "org.apache.spark.deploy.master.Master"
       "-h" "master"))

(defn start-worker [pwd]
  (sh! "docker" "run" "-d"
       "--name" "worker"
       "--link" "master"
       "-h" "worker"
       "--expose" "7012-7016"
       "--expose" "8881"
       "-p" "8081:8081"
       "-e" "SPARK_CONF_DIR=/conf"
       "-v" (str pwd "/conf/worker:/conf")
       "-v" (str pwd "/data:/tmp/data")
       "-e" "SPARK_WORKER_CORES=2"
       "-e" "SPARK_WORKER_MEMORY=1g"
       "-e" "SPARK_WORKER_PORT=8881"
       "-e" "SPARK_WORKER_WEBUI_PORT=8081"
       "gettyimages/spark"
       "bin/spark-class"
       "org.apache.spark.deploy.worker.Worker" "spark://master:7077"))

(defn stop-spark [instance]
  (sh! "docker" "stop" instance)
  (sh! "docker" "rm" instance))

(defn with-cluster [f]
  (let [pwd (.getAbsolutePath (java.io.File. ""))]
    (start-master pwd)
    (Thread/sleep 2000)
    (start-worker pwd)
    (Thread/sleep 2000))
  (f)
  (stop-spark "worker")
  (stop-spark "master"))

(defn with-connection [f]
  (keg/connect! "spark://localhost:7077")
  (f)
  (keg/disconnect!))

(use-fixtures :once with-cluster with-connection)

(deftest rdd
  (is (= (into [] (keg/rdd (range 10)))
         (range 10))))
