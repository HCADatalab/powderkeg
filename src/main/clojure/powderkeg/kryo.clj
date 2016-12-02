(ns powderkeg.kryo
  (:require [carbonite.api :as carb]
    [carbonite.serializer :as ser]))

(defn serializer [read write]
  (powderkeg.SerializerStub. read write))

(def default-serializers
  {clojure.lang.ITransientCollection
   (serializer
     (fn read-transient [_ ^com.esotericsoftware.kryo.Kryo kryo ^com.esotericsoftware.kryo.io.Input input class]
       (transient (.readClassAndObject kryo input)))
     (fn write-transient [_ ^com.esotericsoftware.kryo.Kryo kryo ^com.esotericsoftware.kryo.io.Output output coll]
       (.writeClassAndObject kryo output (persistent! coll))))
   clojure.lang.IPersistentMap
   (serializer
     (fn [_ ^com.esotericsoftware.kryo.Kryo kryo ^com.esotericsoftware.kryo.io.Input input class]
       (loop [remaining (.readInt input true)
              m (.newInstance kryo class)]
         (if (zero? remaining)
           m
           (recur (dec remaining)
             (assoc m
               (.readClassAndObject kryo input)
               (.readClassAndObject kryo input))))))
     (fn [_ ^com.esotericsoftware.kryo.Kryo kryo ^com.esotericsoftware.kryo.io.Output output coll]
       (ser/write-map kryo output coll)))})

(defn register-default-serializers [^com.esotericsoftware.kryo.Kryo kryo m]
  (doseq [[^Class class ^com.esotericsoftware.kryo.Serializer serializer] m]
    (.addDefaultSerializer kryo class serializer)))

(defn customizer [^com.esotericsoftware.kryo.Kryo kryo]
  (doto kryo
    carb/default-registry
    (register-default-serializers default-serializers)
    (.setInstantiatorStrategy (org.objenesis.strategy.StdInstantiatorStrategy.)) ; required for closures
    (.setClassLoader (.getContextClassLoader (java.lang.Thread/currentThread)))))

(defn freeze [x]
  (-> (org.apache.spark.SparkEnv/get) .serializer .newInstance (.serialize x (.AnyRef scala.reflect.ClassTag$/MODULE$)) .array))

(defn unfreeze [bytes]
  (-> (org.apache.spark.SparkEnv/get) .serializer .newInstance (.deserialize (java.nio.ByteBuffer/wrap bytes) (.AnyRef scala.reflect.ClassTag$/MODULE$))))