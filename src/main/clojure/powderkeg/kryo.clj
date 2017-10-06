(ns powderkeg.kryo
  (:require [carbonite.api :as carb]
    [carbonite.serializer :as ser]))

(defn serializer [read write]
  (powderkeg.SerializerStub. read write))

(defn- serializer-factory [f]
 (reify com.esotericsoftware.kryo.factories.SerializerFactory
   (makeSerializer [factory kryo class] (f kryo class))))

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
       (ser/write-map kryo output coll)))
   Class
   (serializer
     (fn read [_ ^com.esotericsoftware.kryo.Kryo kryo ^com.esotericsoftware.kryo.io.Input input _]
       (let [class (some-> (.readClass kryo input) .getType)
             is-primitive (.read input)]
         (cond-> class
           (and (some-> class .isPrimitive) (zero? is-primitive))
           com.esotericsoftware.kryo.util.Util/getWrapperClass)))
     (fn write [_ ^com.esotericsoftware.kryo.Kryo kryo ^com.esotericsoftware.kryo.io.Output output ^Class class]
       (.writeClass kryo output class)
       (.writeByte output (if (some-> class .isPrimitive) 1 0))))
   java.util.Collection
    (serializer-factory
      (fn [kryo ^Class class]
        (if (try (.getDeclaredConstructor class (into-array Class nil)) (catch NoSuchMethodException _ nil))
          (com.esotericsoftware.kryo.serializers.CollectionSerializer.)
          (com.esotericsoftware.kryo.serializers.FieldSerializer. kryo class))))})

(def void-serializer (serializer (fn [_ _ _ _]) (fn [_ _ _ _])))

(defn register-default-serializers [^com.esotericsoftware.kryo.Kryo kryo m]
  (doseq [[^Class class serializer] m]
    (.addDefaultSerializer kryo class serializer)))

(defn customizer [^com.esotericsoftware.kryo.Kryo kryo]
  (doto kryo
    carb/default-registry
    (.register Void/TYPE void-serializer)
    (register-default-serializers default-serializers)
    (.setInstantiatorStrategy (org.objenesis.strategy.StdInstantiatorStrategy.)) ; required for closures
    (.setClassLoader (.getContextClassLoader (java.lang.Thread/currentThread)))))

(defn freeze [x]
  (-> (org.apache.spark.SparkEnv/get) .serializer .newInstance (.serialize x (.AnyRef scala.reflect.ClassTag$/MODULE$)) .array))

(defn freezable? [x]
  (try (freeze x) true (catch Exception _ false)))

(defn unfreeze [bytes]
  (-> (org.apache.spark.SparkEnv/get) .serializer .newInstance (.deserialize (java.nio.ByteBuffer/wrap bytes) (.AnyRef scala.reflect.ClassTag$/MODULE$))))