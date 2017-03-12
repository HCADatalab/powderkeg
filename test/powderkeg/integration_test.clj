(ns powderkeg.integration-test
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]))

(defn sh! [& args]
  (let [{:keys [exit out err] :as ret} (apply sh args)]
    (when-not (zero? exit)
      (throw (Exception. (str "Problem while running '" (s/join " " args) "': " out " " err))))
    (.trim out)))

(defn start-master [pwd version]
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
       (str "gettyimages/spark:" version)
       "/usr/spark/bin/spark-class"
       "org.apache.spark.deploy.master.Master"
       "-h" "master"))

(defn start-worker [pwd version]
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
       (str "gettyimages/spark:" version)
       "/usr/spark/bin/spark-class"
       "org.apache.spark.deploy.worker.Worker" "spark://master:7077"))

(defn stop-spark [instance]
  (sh! "docker" "stop" instance)
  (when-not (System/getenv "CIRCLECI")
    (sh! "docker" "rm" instance)))

(defmacro with-resource
  "Setup resources and tear them down after running body.

  Takes a function, which when called, will setup necessary resources,
  and returns a function, which when called, will tear the resources down"
  [setup & body]
  `(let [teardown# (~setup)]
     (try
       (do ~@body)
       (finally (teardown#)))))

(defn start-spark [version]
  (let [pwd (.getAbsolutePath (java.io.File. ""))]
    (start-master pwd version)
    (Thread/sleep 2000)
    (start-worker pwd version)
    (Thread/sleep 2000)))

(defn stop-cluster []
  (stop-spark "worker")
  (stop-spark "master"))

(defn spark [version]
  (fn []
    (start-spark version)
    stop-cluster))

(defn clojure-dynamic-classloader []
  (let [cl (.getContextClassLoader (Thread/currentThread))]
    (.setContextClassLoader (Thread/currentThread) (clojure.lang.DynamicClassLoader. cl))
    #(.setContextClassLoader (Thread/currentThread) cl)))

(defn keg-connection []
  (keg/connect! "spark://localhost:7077")
  #(keg/disconnect!))

(deftest ^:integration rdd-spark-2.1.0
  (with-resource
    (spark "2.1.0-hadoop-2.7")
    (with-resource
      clojure-dynamic-classloader
      (with-resource
        keg-connection
        (is (= (into [] (keg/rdd (range 10)))
               (range 10)))))))

(deftest ^:integration rdd-spark-1.5.2
  (with-resource
    (spark "1.5.2-hadoop-2.6")
    (with-resource
      clojure-dynamic-classloader
      (with-resource
        keg-connection
        (is (= (into [] (keg/rdd (range 10)))
               (range 10)))))))
