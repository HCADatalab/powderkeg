(ns powderkeg.repl
  (:gen-class :main true)
  (:require clojure.main))

(defn -main [& args]
  (let [main (first args)]
    (apply clojure.main/main (concat
                               (mapcat (fn [e] ["-e" (pr-str e)])
                                 ['(require '[powderkeg.core :as keg] '[net.cgrand.xforms :as x])
                                  '(-> (org.apache.log4j.LogManager/getRootLogger) (.setLevel org.apache.log4j.Level/WARN))
                                  '(keg/connect!)
                                  (when main
                                    `(do
                                       (require '~(namespace (symbol main)))
                                       (~main ~@(next args))))])
                               (when-not main ["-r"])))))