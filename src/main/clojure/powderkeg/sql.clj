(ns powderkeg.sql
  (:require [clojure.spec :as s]
    [clojure.edn :as edn]
    [powderkeg.core :as keg]
    [net.cgrand.xforms :as x])
  (:import
    [org.apache.spark.sql functions Column Row RowFactory DataFrame]
    [org.apache.spark.sql.types StructType StructField ArrayType DataType DataTypes Metadata MetadataBuilder]))

(defmulti expr first)

(defn- scala-seq [x]
  (-> x sequence scala.collection.JavaConversions/asScalaBuffer .toList))

(defn- metatag [x]
  (-> (MetadataBuilder.) (.putString "powderkeg.sql" (pr-str x)) .build))

(def ^:private regexp-repeat
  (s/and 
    (s/cat :tag any? :mapping ::mapping)
    (s/conformer
      (fn [{{::keys [schema to-sql from-sql tag src]} :mapping tag :tag}]
        {::schema (DataTypes/createArrayType schema)
         ::src (list tag src)
         ::to-sql #(scala-seq (map to-sql %))
         ::from-sql #(into [] (map from-sql) (scala.collection.JavaConversions/seqAsJavaList ^scala.collection.Seq %))}))))

(defmethod expr `s/every [_] (s/cat :tag any? :type ::mapping :options (s/* any?))) ; TODO once every is fixed
(defmethod expr `s/keys [_]
  (s/conformer
    (fn [[_ & {:keys [req req-un opt opt-un] :as opts}]]
      (let [fields
            (-> {}
              (into (comp (remove symbol?) (map #(vector % (s/conform ::mapping %))))
                (flatten (concat req opt)))
              (into (comp (remove symbol?) (map #(vector (keyword (name %)) (s/conform ::mapping %))))
                (flatten (concat req-un opt-un))))]
        (or (some #{::s/invalid} (vals fields))
          {::src `(s/keys ~@(apply concat (select-keys opts [:req :req-un :opt :opt-un])))
           ::schema
           (DataTypes/createStructType
             ^java.util.List (map (fn [[k v]] (DataTypes/createStructField (subs (str k) 1) (::schema v) true (metatag (::src v)))) fields))
           ::to-sql
           (fn [m]
             (RowFactory/create
               (object-array (map (fn [[k {::keys [to-sql]}]] (to-sql (k m))) fields))))
           ::from-sql
           (fn [^Row row]
             (into {} (map (fn [[k {::keys [from-sql]}]]
                             (let [f (subs (str k) 1)]
                               [k (from-sql (.getAs row f))]))) fields))})))))
(defmethod expr `s/tuple [_]
  (s/conformer
    (fn [[_ & specs]]
      (let [schemas (map #(s/conform ::mapping %) specs)]
        (or (some #{::s/invalid} schemas)
          {::schema
           (DataTypes/createStructType
            ^java.util.List (map-indexed (fn [i v] (DataTypes/createStructField (str i) (::schema v) false (metatag (::src v)))) schemas))
           ::to-sql
           (fn [v]
             (RowFactory/create
               (object-array (map-indexed (fn [i {::keys [to-sql]}] (to-sql (nth v i))) schemas))))
           ::from-sql
           (fn [^Row row]
             (into [] (map-indexed (fn [i {::keys [from-sql]}]
                                     (from-sql (.get row i)))) schemas))})))))
(defmethod expr `s/* [_] regexp-repeat)
(defmethod expr `s/+ [_] regexp-repeat)

(def preds-registry {`string? DataTypes/StringType
                     `boolean? DataTypes/BooleanType
                     `double? DataTypes/DoubleType
                     `int? DataTypes/LongType
                     `nil? DataTypes/NullType
                     `inst? DataTypes/TimestampType})

(def types-registry (into {DataTypes/IntegerType `int?}
                      (map (fn [[k v]] [v k]))
                      preds-registry))

(s/def ::mapping
  (s/and
    (s/or
     :named (s/and qualified-keyword? 
              (s/conformer
                (fn [k]
                  (if-some [form (some-> k s/get-spec s/form)]
                    (s/conform (s/and ::mapping (s/conformer #(assoc % ::src k))) form)
                    ::s/invalid))))
     :expr (s/and seq? (s/multi-spec expr (fn [form tag] (cons tag (next form)))))
     :pred (s/and qualified-symbol?
             (s/conformer #(if-some [t (preds-registry %)]
                             {::schema t ::src % ::to-sql identity ::from-sql identity}
                             ::s/invalid)))
     :spec-object (s/and s/spec? (s/conformer #(s/conform ::mapping (s/form %))))
     :regexp (s/and (s/keys :req [::s/op]) (s/conformer #(s/conform ::mapping (s/form %)))))
    (s/conformer val)))

(defn df [in spec]
  (let [{::keys [schema to-sql]} (s/conform ::mapping spec)
        sql-ctx (org.apache.spark.sql.SQLContext/getOrCreate (.sc keg/*sc*))]
    (.createDataFrame sql-ctx (keg/rdd in (map to-sql)) schema)))

(defn exec [query]
  (.sql (org.apache.spark.sql.SQLContext/getOrCreate (.sc keg/*sc*)) query))

;; reverse inference

(defn to-spec [dt]
  (cond
    (instance? StructType dt)
    (let [specs (into {}
                  (map (fn [^StructField sf]
                         [(edn/read-string (str ":" (.name sf))) 
                          (let [meta (.metadata sf)]
                            (if (.contains meta "powderkeg.sql")
                              (edn/read-string (.getString meta "powderkeg.sql"))
                              (to-spec (.dataType sf))))]))
                  (.fields ^StructType dt))
          {:keys [adhoc] :as opts}
          (group-by (fn [[k spec]] (cond
                                     (= k spec) :req
                                     (and (simple-keyword? k) (qualified-keyword? spec)
                                       (= (name k) (name spec))) :req-un
                                     :else :adhoc)) specs)
          opts (mapcat (fn [[k specs]] [k (vec (vals specs))]) (dissoc opts :adhoc))
          skeys (when (seq opts) `(s/keys ~@opts))
          adhoc (when (seq adhoc) `(adhoc-keys ~@(apply concat adhoc)))]
      (or
        (and skeys adhoc `(s/merge ~skeys ~adhoc)) 
        skeys adhoc `any?))
    (instance? ArrayType dt) (list `s/* (to-spec (.elementType ^ArrayType dt)))
    :else (types-registry dt `any?)))

(defn spec-of [^DataFrame df]
  (eval (to-spec (.schema df))))

(defmacro adhoc-keys
  "Don't use. adhoc-keys is against the spirit of clojure.spec, it's only for \"retrospecing\"
   alien schemas."
  [& k+specs]
  `(adhoc-keys-impl ~(vec k+specs) '~k+specs))

(defn adhoc-keys-impl
  "Do not call this directly, use 'adhoc-keys'."
  [k+preds k+forms]
  (reify
   s/Specize
   (specize* [s] s)
   (specize* [s _] s)
   
   s/Spec
   (conform* [_ x] (reduce-kv (fn [m k pred]
                                (if (contains? x k)
                                  (let [v (s/conform pred (k x))]
                                    (if (s/invalid? v)
                                      (reduced ::s/invalid)
                                      (assoc m k v)))
                                  (reduced ::s/invalid)))
                     {} k+preds))
   (unform* [_ x] (reduce-kv (fn [m k pred]
                                (assoc m k (s/unform pred (k x))))
                     {} k+preds))
   #_(explain* [_ path via in x] )
   #_(gen* [_ overrides path rmap]
          (if gfn
            (gfn)
            (gen/fmap
             #(apply c/merge %)
             (apply gen/tuple (map #(gensub %1 overrides path rmap %2)
                                   preds forms)))))
   #_(with-gen* [_ gfn] (merge-spec-impl forms preds gfn))
   (describe* [_] `(adhoc-keys ~@k+forms))))

(defn- m [^java.lang.reflect.Method m]
  (and  (= Column (.getReturnType m))
    (every? (fn [^Class class]
              (or (.isPrimitive class) (#{String Column} class)
                )) (.getParametersType m))))

(let [methods (for [^java.lang.reflect.Method m (concat (.getMethods Column) (.getMethods functions))
                   :let [class (.getDeclaringClass m)]
                   :when (#{Column functions} class)]
                [class (.getName m) (cond->> (seq (.getParameterTypes m))
                                      (zero? (bit-and java.lang.reflect.Modifier/STATIC (.getModifiers m))) (cons class))])
      varargs (into {} (x/by-key second (comp (filter #(some-> (last (peek %)) .isArray))
                                          (x/into [])
                                          (filter next))) methods)]
  varargs)

(def syms-to-methods
  (-> {}
    (into (comp (filter #(= Column (.getReturnType %)))
            (map (fn [m]
                   [(symbol (.getName m)) (symbol "org.apache.spark.sql.functions" (.getName m))]))) (.getMethods functions))
    (into (comp (filter #(= Column (.getReturnType %)))
            (map (fn [m]
                   [(symbol (.getName m)) (symbol (str "." (.getName m)))]))) (.getMethods Column))))

(defn to-col [x]
  (cond
    (seq? x) (cons (syms-to-methods (first x))
               (map to-col (rest x)))
    (keyword? x) (Column. (subs (str x) 1))))