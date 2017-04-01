(ns powderkeg.docker
  (:require [powderkeg.core :as keg]
            [clojure.test :refer :all]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as s]))

(defmacro timing [log-prefix & body]
  `(do
     (println (str ~log-prefix " starting"))
     (let [start# (System/currentTimeMillis)
           result# (do ~@body)
           end# (System/currentTimeMillis)
           duration# (- end# start#)]
       (println (str ~log-prefix " took: " (- end# start#) "ms" ))
       {:result result#
        :duration duration#})))

(defn sh! [& args]
  (let [{:keys [exit out err] :as ret} (apply sh args)]
    (when-not (zero? exit)
      (throw (Exception. (str "Problem while running '" (s/join " " args) "': " out " " err))))
    (.trim out)))

(defn path-to-spark-class [version]
  (condp = version
    "2.1.0-hadoop-2.7" "/usr/spark-2.1.0/bin/spark-class"
    "1.5.2-hadoop-2.6" "/usr/spark/bin/spark-class"))

(defn start-master [pwd version]
  (sh! "docker" "run" "-d"
       "--name" (str "master-" version)
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
       (path-to-spark-class version)
       "org.apache.spark.deploy.master.Master"
       "-h" "master"))

(defn start-worker [pwd version]
  (sh! "docker" "run" "-d"
       "--name" (str "worker-" version)
       "--link" (str "master-" version)
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
       (path-to-spark-class version)
       "org.apache.spark.deploy.worker.Worker" "spark://master:7077"))

(defn stop-spark [instance]
  (sh! "docker" "stop" instance)
  (when-not (System/getenv "CIRCLECI")
    (sh! "docker" "rm" instance)))

(defn start-cluster [version]
  (let [pwd (.getAbsolutePath (java.io.File. ""))]
    (timing "Master startup"
      (start-master pwd version))
    (Thread/sleep 2000)
    (timing "Worker startup"
      (start-worker pwd version))
    (Thread/sleep 2000)))

(defn stop-cluster [version]
  (stop-spark (str "worker-" version))
  (stop-spark (str "master-" version)))

(defn spark [version]
  (fn []
    (start-cluster version)
    #(timing "Cluster shutdown"
       (stop-cluster version))))
