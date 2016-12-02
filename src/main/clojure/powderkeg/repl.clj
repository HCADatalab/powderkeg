(ns powderkeg.repl
  (:gen-class :main true)
  (:require clojure.main))

(defn -main [& args]
  (apply clojure.main/main ["-e" "(require '[powderkeg.core :as keg] '[net.cgrand.xforms :as x])"
                            "-e" "(-> (org.apache.log4j.LogManager/getRootLogger) (.setLevel org.apache.log4j.Level/WARN))"
                            "-e" "(do (keg/connect!) nil)"
                            "-r"]))
