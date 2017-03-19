(ns powderkeg.fixtures
  (:require [powderkeg.core :as keg]))

(defn with-local [f]
  (keg/connect! "local[2]")
  (f)
  (keg/disconnect!))

