(ns powderkeg.fixtures
  (:require [powderkeg.core :as keg]))

(defmacro with-resources
  "Setup resources and tear them down after running body.
  Takes a function, which when called, will setup necessary resources,
  and returns a function, which when called, will tear the resources down.

  Can be given multiple setup functions, which are called in order"
  [setups & body]
  (if-some [[setup & setups] (seq setups)]
    `(let [teardown# (~setup)]
       (try
         (with-resources [~@setups] ~@body)
         (finally (teardown#))))
    `(do ~@body)))

(defn local-spark []
  (keg/connect! "local[2]")
  #(keg/disconnect!))

(defn keg-connection [host]
  (fn []
    (keg/connect! (str "spark://" host ":7077"))
    #(keg/disconnect!)))

(defn clojure-dynamic-classloader []
  (let [cl (.getContextClassLoader (Thread/currentThread))]
    (.setContextClassLoader (Thread/currentThread) (clojure.lang.DynamicClassLoader. cl))
    #(.setContextClassLoader (Thread/currentThread) cl)))
