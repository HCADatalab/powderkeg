(ns powderkeg.repl
  (:gen-class :main true)
  (:require clojure.main))

(defn -main [& args]
  (let [main (some-> args first symbol)]
    (apply clojure.main/main (concat
                               (mapcat (fn [e] ["-e" (pr-str e)])
                                 ['(require '[powderkeg.core :as keg] '[net.cgrand.xforms :as x])
                                  '(-> (org.apache.log4j.LogManager/getRootLogger) (.setLevel org.apache.log4j.Level/WARN))
                                  '(keg/connect!)
                                  (when main
                                    `(do
                                       (require '~(symbol (namespace main)))
                                       (~main ~@(next args))))])
                               (when-not main ["-r"])))))