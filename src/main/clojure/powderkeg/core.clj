(ns powderkeg.core
  "A spark lib."
  (:refer-clojure :exclude [into shuffle])
  (:require 
    [powderkeg.ouroboros :as ou] ; must be first
    [powderkeg.kryo :as kryo]
    [powderkeg.pool :as pool]
    [cemerick.pomegranate.aether :as mvn]
    [clojure.core :as clj]
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.core.protocols :refer [coll-reduce CollReduce]]
    [clojure.core.reducers :as r]
    [net.cgrand.xforms :as x]))

(defn- all-files
  "Returns a map of relative paths (as Strings) to Files for all files (not directories) below the argument."
  [^java.io.File f]
  (let [f (.getCanonicalFile f)
        path (.getCanonicalPath f)
        dir? #(.isDirectory ^java.io.File %)
        dot? #(.startsWith (.getName ^java.io.File %) ".")]
    (clj/into {}
      (comp
        (remove (some-fn dir? dot?))
        (keep #(let [p (.getCanonicalPath ^java.io.File %)]
                 ; the test below is for links, I didn't check how they behave with list and canonpath
                 (when (.startsWith p path) [(subs p (inc (count path))) %]))))
      (tree-seq (every-pred dir? (complement dot?)) #(.listFiles ^java.io.File %) f))))

(defn package-env!
  "Package (as a single jar) all dirs on the classpath. Returns a File pointing to the resulting jar."
  []
  (let [content (clj/into {}
                 (comp
                   (map #(java.io.File. (.toURI %)))
                   (filter #(.isDirectory %))
                   (map all-files))
                 (.getURLs (java.lang.ClassLoader/getSystemClassLoader)))
        f (ou/tmp-file "remaining-env-" ".jar")]
    (with-open [out (io/output-stream f)]
      (ou/jar! out {} content))
    f))

(defn guess-all-jars-but-spark
  "Get a list (of URLs) of jars on the classpath which does not belong to Spark or its dependencies."
  []
  (let [system-jars (clj/into #{}
                      (comp
                        (map #(java.io.File. (.toURI %)))
                        (filter #(and (.isFile %) (.endsWith (.getName %) ".jar"))))
                      (.getURLs (java.lang.ClassLoader/getSystemClassLoader)))
        coords (clj/into []
                 (comp
                   (map #(.getName %))
                   (keep #(re-matches #"(spark-.*)-(\d+\.\d+\.\d+(?:-.*)?)\.jar" %))
                   (map (fn [[_ p v]] [(symbol "org.apache.spark" p) v])))
                 system-jars)
        spark-jars (set (mvn/dependency-files (mvn/resolve-dependencies :retrieve true :coordinates coords)))
        other-jars (reduce disj system-jars spark-jars)]
    (when (= other-jars system-jars)
      (throw (ex-info "Can't filter out Spark jars!" {})))
    other-jars))

; environment changes are chained and broadcasted, this is a reference to the head of the chain
(def ^:private last-broadcast (atom nil))

(declare ^:dynamic ^:powderkeg/no-sync ^org.apache.spark.api.java.JavaSparkContext *sc*)

(defn- alter-var [v f & args]
  (if (thread-bound? v)
        (var-set v (apply f @v args))
        (apply alter-var-root v f args)))

(defn connect!
  "Connects to a spark cluster. Closes existing connection.
   The no-arg version will uses the default settings (as set for example by spark-submit)."
  ([] (connect! nil))
  ([master-url]
    (alter-var #'*sc*
      (fn [sc]
        (try
          (.close sc)
          (catch Exception _))
        (ou/reboot!)
        (reset! last-broadcast nil)
        (let [conf (-> (org.apache.spark.SparkConf.) (.setAppName "repl")
                     (cond-> master-url (.setMaster master-url))
                     (.set "spark.driver.allowMultipleContexts" "true")
                     (.set "spark.serializer" "org.apache.spark.serializer.KryoSerializer")
                     (.set "spark.kryo.registrator", "powderkeg.KryoCustomizer"))]
          (when (empty? (.get conf "spark.jars" nil))
            ; not spark-submitted
            (.setJars conf
              (into-array
                (map #(str (.toURL %))
                  (conj
                    (guess-all-jars-but-spark)
                    (package-env!))))))
          (org.apache.spark.api.java.JavaSparkContext. conf))))))

(defn disconnect! []
  (alter-var #'*sc*
    (fn [sc]
      (try
        (.close sc)
        (catch Exception _))
      nil)))

#_ (connect! "spark://macbook-pro.home:7077")

;; the delayed swap is a trick to perform side effects (or long computations) while using an atom:
;; the swap! determins who wins the write then anybody can force the realization and it will happen only once.
(defn- delayed-swap! [atom f & args]
  @(swap! atom (fn [d]
                 (delay (apply f (force d) args)))))

(defn barrier!
  "Emits a repl-state (class/var definition) barrier: all dynamicly defined classes are sent to the cluster and a fn is returned.
   When called this fn updates vars and namespaces."
  []
  (let [f (ou/tmp-file "barrier-" ".jar")
        {:keys [classes vars]} (ou/latest-changes!)]
    (with-open [out (io/output-stream f)]
      (ou/jar! out {}
        (map (fn [[classname bytes]] [(str classname ".class") bytes]) classes)))
    (.addJar *sc* (str (.toURL f)))
    (if-not (= {} vars)
      (let [bc (delayed-swap! last-broadcast
                 (fn [parent-bc]
                   (.broadcast *sc* 
                     (delay
                       (some-> parent-bc .value force) ; that's how env changes are chained
                       (doseq [[ns-sym vars] vars
                               :let [ns (create-ns ns-sym)]
                               [sym [{:keys [dynamic] :as m} v]] vars]
                         (doto ^clojure.lang.Var (intern ns (with-meta sym m) v)
                           (.setDynamic (boolean dynamic))))))))]
        #(force (.value bc)))
      (let [bc @@last-broadcast]
        #(force (.value bc))))))

;; core API
;; The main object is the RDD
;; a RDD is reducible and foldable (TODO)
;; reduce should process one partition at a time

(defprotocol RDDable
  (-rdd [src sc opts] "Creates a RDD, may be a noop if src is already a RDD and no options are provided."))

(extend-protocol RDDable
  Object
  (-rdd [x ^org.apache.spark.api.java.JavaSparkContext sc opts]
    (.parallelize sc (if (instance? java.util.List x) x (clj/into [] x))
      (:partitions opts (.defaultParallelism sc))))
  nil
  (-rdd [x ^org.apache.spark.api.java.JavaSparkContext sc opts]
    (.emptyRDD sc))
  org.apache.spark.api.java.JavaRDD
  (-rdd [rdd sc opts]
    (if-some [n (:partitions opts)]
      (let [p (count (.partitions rdd))]
        (cond
          (= n p) rdd
          (< n p) (.coalesce rdd n)
          :else (.repartition rdd n)))
      rdd)) ; todo: look at options for eg repartitioning
  org.apache.spark.api.java.JavaPairRDD
  (-rdd [rdd sc opts]
    (-> rdd .rdd (-rdd sc opts)))
  org.apache.spark.rdd.RDD
  (-rdd [rdd sc opts]
    (-> rdd
      (org.apache.spark.api.java.JavaRDD/fromRDD (.AnyRef scala.reflect.ClassTag$/MODULE$))
      (-rdd sc opts))))

(defn- barrier-fn
  "Creates a java-serializable caching thunk which returns f upon invocation."
  [f]
  ; closures are not serialized the same way as data by spark, the goal of this thunk is
  ; to control the closure serialization and go through kryo (to avoid serialization bailing out
  ; as soon as we close over a non java.io.Serializable).
  (let [update-vars! (barrier!)
        bytes (kryo/freeze f) ; the true closure is serialized by kryo
        cache (object-array [nil bytes])]
    (fn []
      (if-some [f (aget cache 0)]
        f
        (locking cache
          (when-not (aget cache 0)
            (update-vars!)
            (when-some [bytes (aget cache 1)]
              (doto cache (aset 0 (kryo/unfreeze bytes)) (aset 1 nil))))
          (aget cache 0))))))

(defn- may-map-tuple2
  "Stateless transducer. Returns a reducing function that when called with arity 2 with a 2-item vector oe with airty 3 creates a Tuple2."
  [rf]
  (x/kvrf
    ([] (rf))
    ([acc] (rf acc))
    ([acc x] (if (and (vector? x) (= 2 (count x)))
               (rf acc (scala.Tuple2. (nth x 0) (nth x 1)))
               (rf acc x)))
    ([acc k v] (rf acc (scala.Tuple2. k v)))))

(defn- unmap-tuple2
  [rf]
  (if-some [rf (x/some-kvrf rf)]
    (fn 
      ([] (rf))
      ([acc] (rf acc))
      ([acc ^scala.Tuple2 x] (rf acc (._1 ^scala.Tuple2 x) (._2 ^scala.Tuple2 x))))
    (fn 
      ([] (rf))
      ([acc] (rf acc))
      ([acc ^scala.Tuple2 x] (rf acc (clojure.lang.MapEntry. (._1 ^scala.Tuple2 x) (._2 ^scala.Tuple2 x)))))))

(defn- may-unmap-tuple2
  "Stateless transducer that, when passed in a Tuple2, decomposes it when the downstream reducing function supports arity 3 else maps it to a map-entry."
  [rf]
  (if-some [rf (x/some-kvrf rf)]
    (fn 
      ([] (rf))
      ([acc] (rf acc))
      ([acc x] (if (instance? scala.Tuple2 x)
                 (rf acc (._1 ^scala.Tuple2 x) (._2 ^scala.Tuple2 x))
                 (rf acc x))))
    (fn 
      ([] (rf))
      ([acc] (rf acc))
      ([acc x] (rf acc (if (instance? scala.Tuple2 x)
                         (clojure.lang.MapEntry. (._1 ^scala.Tuple2 x) (._2 ^scala.Tuple2 x))
                         x))))))

(defn ^org.apache.spark.api.java.JavaRDD ensure-rdd
  "Converts (when needed) src to a rdd."
  ([src] (ensure-rdd src {}))
  ([src options]
    (-rdd src (:sc options *sc*) options)))

(defn ^org.apache.spark.api.java.JavaRDD rdd*
  "Like rdd without the mixed varargs."
  [src xform options-map]
  (let [{:keys [preserve-partitioning] :or {preserve-partitioning false} :as options} options-map
        rdd (ensure-rdd src options)
        df (barrier-fn (fn [it] (eduction (comp may-unmap-tuple2 xform may-map-tuple2) (iterator-seq it))))
        rdd (.mapPartitions rdd
              (reify org.apache.spark.api.java.function.FlatMapFunction ; todo: skip api.java.* go to spark
                (call [_ it] ((df) it)))
              preserve-partitioning)]
    rdd))

(def ^:private sink
  (x/kvrf ([] nil) ([_] nil) ([_ _] nil) ([_ _ _] nil) ([_ _ _ _] nil)))

(defn do-rdd*
  "Like rdd* but for forcing computation."
  [src xform options]
  (let [rdd (ensure-rdd src options)
        df (barrier-fn (fn [it] (transduce (comp may-unmap-tuple2 xform) sink (iterator-seq it))))
        rdd (.foreachPartition rdd
              (reify org.apache.spark.api.java.function.VoidFunction ; todo: skip api.java.* go to spark
                (call [_ it] ((df) it))))]
    rdd))

(defn ^org.apache.spark.api.java.JavaRDD rdd
  "Creates a RDD from applying transducers (xforms) to a source (src).
   Transducers may be followed by options.
   Supported options are:
   :preserve-partitioning bool"
  [src & xforms-then-options]
  (let [[xforms {:as options}] (split-with (complement keyword?) xforms-then-options)
        xform (apply comp xforms)]
    (rdd* src xform options)))

(defn ^org.apache.spark.api.java.JavaRDD shuffle
  "Shuffles a pair rdd. The partitioner argument may be either:
   * true (default for 1-arg) — use hash partitioning with default parallelism,
   * N (number) – use hash partitioning with N partitions,
   * nil or false – do nothing,
   * partitioner instance – use it.
   To change the partitioning of non-pair rdds, use rdd with the :partitions option."
  ([pair-rdd]
    (shuffle pair-rdd true))
  ([^org.apache.spark.api.java.JavaRDD pair-rdd partitioner]
    (if-some [partitioner (cond 
                            (number? partitioner) (org.apache.spark.HashPartitioner. partitioner)
                            (true? partitioner) (org.apache.spark.HashPartitioner. (-> pair-rdd .context .defaultParallelism))
                            (false? partitioner) nil
                            :else partitioner)]
      ; ok I think it's proof that the Java API is getting in our way
      (-> pair-rdd
        org.apache.spark.api.java.JavaPairRDD/fromJavaRDD
        (.partitionBy partitioner)
        ensure-rdd)
      pair-rdd)))

(defn- unmix-options 
  "Parses an out of order mix of xforms and options. Anything preceded by a keyword is an option.
   Returns a pair of [xforms options] where xforms is a sequential collection and options a map."
  [xforms-and-options]
  (reduce (fn [[xforms options k] x]
            (cond
              k [xforms (assoc options k x)]
              (keyword? x) [xforms options x]
              :else [(conj xforms x) options]))
    [[] {}] xforms-and-options))

(defn- or-default [value default-value]
  (case value ::default default-value value))

(defn ^org.apache.spark.api.java.JavaRDD by-key [src & xforms-and-options]
  "Transforms and shuffle a pair RDD. (Accepts non-pair RDD too when you provide a :key function.)
   Transformations are applied on both side. You can specify additional map-side
   transformations (:pre) and reduce-side transformation (:post)
   Partitioning is constrolled by the :shuffle key which defaults to true
   (unless :key is not provided and the source rdd is already partitioned)."
  (let [[xforms {:keys [pre post key preserve-partitioning] :as options
                 shuffle-opt :shuffle
                 :or {preserve-partitioning ::default
                      shuffle-opt ::default}}] (unmix-options xforms-and-options)
        options (dissoc options :shuffle :post :pre :key :preserve-partitioning) ; dissoc options consumed by by-key
        src (ensure-rdd src options)
        shuffle-opt (or-default shuffle-opt (or (some? key) (-> src .partitioner .orNull nil?))) ; by default shuffle when key is provided or when the source is not partitioned yet
        preserve-partitioning (or-default preserve-partitioning (not shuffle-opt)) ; by default preserve partitioning when no shuffle
        options (assoc options :preserve-partitioning preserve-partitioning)
        xform (apply comp xforms)]
    (if shuffle-opt
      (let [map-side-xform (if key 
                             (x/by-key key (cond->> xform pre (comp pre)))
                             (x/by-key (cond->> xform pre (comp pre))))
            rdd (-> src (rdd* map-side-xform options) (shuffle shuffle-opt))]
        (if-some [reduce-side-xform (when (or (seq xforms) post)
                                      (x/by-key (cond-> xform post (comp post))))]
          (rdd* rdd reduce-side-xform (assoc options :preserve-partitioning true))
          rdd))
      ; no shuffle
      (let [xform (x/by-key (-> xform (cond-> post (comp post)) (cond->> pre (comp pre))))]
        (rdd* src xform options)))))

(defn- default-left
  "Returns a stateless transducer on pairs which expects Optionals in key position, unwraps their values or return not-found when no value."
  [not-found]
  (fn [rf]
    (let [rf (x/ensure-kvrf rf)]
      (x/kvrf
        ([] (rf))
        ([acc] (rf acc))
        ([acc left right] (rf acc (.or ^com.google.common.base.Optional left not-found) right))))))

(defn- default-right
  "Returns a stateless transducer on pairs which expects Optionals in value position, unwraps their values or return not-found when no value."
  [not-found]
  (fn [rf]
    (let [rf (x/ensure-kvrf rf)]
     (x/kvrf
       ([] (rf))
       ([acc] (rf acc))
       ([acc left right] (rf acc left (.or ^com.google.common.base.Optional right not-found)))))))

(defn ^org.apache.spark.api.java.JavaRDD join
  "Performs a join between two rdds, each rdd may be followed by ':or default-value'.
   This determines if the join is inner or full/left/right outer."
  {:arglists '([a (:or not-found)? b (:or not-found)? & xforms-then-options])}
  [a b & xforms-then-options]
  (let [args (list* a b xforms-then-options)
        options {}
        [a & args] args
        [options args] (if (= :or (first args))
                             [(assoc options :or-left (second args)) (nnext args)]
                             [options args])
        [b & args] args
        [options args] (if (= :or (first args))
                             [(assoc options :or-right (second args)) (nnext args)]
                             [options args])
        [xforms {:as rem-options}] (split-with (complement keyword?) args)
        options (clj/into rem-options options)
        unbox (cond-> unmap-tuple2
                (contains? options :or) (comp (default-left (:or options)) (default-right (:or options)))
                (contains? options :or-left) (comp (default-left (:or-left options)))
                (contains? options :or-right) (comp (default-right (:or-right options))))
        xform (apply comp unbox xforms)
        a (org.apache.spark.api.java.JavaPairRDD/fromJavaRDD a)
        b (org.apache.spark.api.java.JavaPairRDD/fromJavaRDD b)
        kvrdd (cond
                 (or (contains? options :or) (and (contains? options :or-left) (contains? options :or-right))) (.fullOuterJoin a b)
                 (contains? options :or-left) (.rightOuterJoin a b)
                 (contains? options :or-right) (.leftOuterJoin a b)
                 :else (.join a b))]
    (by-key kvrdd xform)))

(defn- scala-seq [x]
  (-> x sequence scala.collection.JavaConversions/asScalaBuffer .toList))

(defn- ensure-scala-pair-rdd [x]
  (-> x (by-key :shuffle nil) .rdd))

(defn ^org.apache.spark.api.java.JavaRDD cogroup [rdd & rdds]
  (let [rdd (ensure-scala-pair-rdd rdd)
        rdds (map ensure-scala-pair-rdd rdds)
        partitioner (org.apache.spark.Partitioner/defaultPartitioner
                      rdd (scala-seq rdds))]
    (by-key (org.apache.spark.rdd.CoGroupedRDD. (scala-seq (cons rdd rdds)) partitioner)
      (map (fn [groups]
             (clj/into [] (map #(scala.collection.JavaConversions/asJavaList %)) groups))))))

(defmacro with-res
  "Returns a \"with-open\" inspired transducer which wraps the specified transducers (xforms) and manages the resources specified by bindings (let-style bindings).
   The right-hand side of each binding may feature the :keep-alive (in seconds), :close and :shared metadata.
   :close determines how to discard an instance (by default the .close method is called), else provide an expression where % names the instance.
   :keep-alive determines how long (in seconds) an instance will be kept around before being discarded.
   :shared when true instructs the system that instances can be safely shared by multiple threads. At any time there won't be more than one instance."
  [bindings & xforms]
  (let [lhss (take-nth 2 bindings)
        exprs (take-nth 2 (next bindings))
        pool-names (map #(gensym (str "pool-" (when (symbol? %) (name %)))) lhss)
        lease-names (map #(if (symbol? %) % (gensym 'lease)) lhss)
        pools (for [[lhs expr] (map vector lhss exprs)
                    :let [{:keys [shared keep-alive close] 
                           :or {close '(.close %) keep-alive 0}} (meta expr)
                          expr (vary-meta expr dissoc :shared :keep-alive :close)]]
                `(~(if shared `pool/share `pool/pool)
                   (fn [] ~expr)
                   (fn [~(with-meta '% (meta lhs))] ~(-> expr meta :close))
                   ~keep-alive))
        return-leases `(do ~@(map list pool-names lease-names))]
    `(let [~@(interleave pool-names pools)]
       (fn [rf#]
         (let [~@(mapcat (fn [lhs lease-name pool-name]
                           (clj/into [lease-name (list pool-name)]
                             (when-not (= lease-name lhs) [lhs lease-name])))
                   lhss lease-names pool-names)
               xform# (comp ~@xforms)
               rf# (xform# rf#)]
           (if-some [rf# (x/some-kvrf rf#)]
             (x/kvrf 
               ([] (try (rf#) (catch Exception e# ~return-leases (throw e#))))
               ([acc#] (try (rf# acc#) (finally ~return-leases)))
               ([acc# x#] (try (rf# acc# x#) (catch Exception e# ~return-leases (throw e#))))
               ([acc# x# k# v#] (try (rf# acc# k# v#) (catch Exception e# ~return-leases (throw e#)))))
             (fn
               ([] (try (rf#) (catch Exception e# ~return-leases (throw e#))))
               ([acc#] (try (rf# acc#) (finally ~return-leases)))
               ([acc# x#] (try (rf# acc# x#) (catch Exception e# ~return-leases (throw e#)))))))))))

(defmacro do-rdd
  "Forces computation and performs side-effects. Bindings are managed as per with-res."
  [src bindings & xforms-then-options]
  (let [[xforms {:as options}] (split-with (complement keyword?) xforms-then-options)]
    `(do-rdd* ~src
       (with-res ~bindings ~@xforms)
       ~options)))

(defn ^org.apache.spark.api.java.JavaRDD persist! 
  ([rdd]
    (persist! rdd (org.apache.spark.storage.StorageLevel/MEMORY_ONLY)))
  ([^org.apache.spark.api.java.JavaRDD rdd lvl]
    (.persist rdd (cond
                    (instance? org.apache.spark.storage.StorageLevel lvl) lvl
                    (or (string? lvl) (keyword? lvl)) (org.apache.spark.storage.StorageLevel/fromString (name lvl))
                    :else (throw (ex-info (str "Unexpected storage level:" (pr-str lvl)) {:lvl lvl}))))))

(defn ^org.apache.spark.api.java.JavaRDD unpersist! 
  ([rdd]
    (unpersist! rdd true))
  ([^org.apache.spark.api.java.JavaRDD rdd blocking]
    (.unpersist rdd blocking)))

(defn- run-job 
  "Run a job (a function receiving a scala iterator) on the given partitions
   and returns an array of results."
  [sc ^org.apache.spark.rdd.RDD rdd f partitions]
  (let [df (barrier-fn f) 
        f (fn [it] ((df) it))]
    (.runJob (.sc sc) rdd
      (reify
        java.io.Serializable
        scala.Function1
        (apply [_ it]
          (f it))
        (compose [f g]
          (scala.Function1$class/compose f g))
        (andThen [f g]
          (scala.Function1$class/andThen f g)))
      (scala.collection.mutable.WrappedArray$ofInt. (int-array partitions))
      (.AnyRef scala.reflect.ClassTag$/MODULE$))))

(defn scomp
  "Stateful comp. Use it instead of comp when composing stateful transducers.
   Only useful when the transducers are used outside of a RDD."
  ([& xforms]
    (apply comp (fn [rf]
                  (let [v (volatile! rf)]
                    (with-meta
                      (fn [& args] (apply @v args))
                      {::proxied-fn v}))) xforms)))

(defn into
  "Like clojure.core/into but the transducing arity calls scomp on xform to make it cluster-safe.
   It also supports arity 1 (completion)."
  ([to] to)
  ([to from]
    (clj/into to from))
  ([to xform from]
    (clj/into to (scomp xform) from)))

(defn reduce-rdd
  [rf init ^org.apache.spark.rdd.RDD rdd]
  (let [v (some-> rf meta ::proxied-fn)
        f (if v (deref v) rf)
        [acc f] (reduce 
                  (fn [[acc f] partition-id]
                    (aget (run-job *sc* rdd (fn [^scala.collection.Iterator it]
                                              (loop [acc acc]
                                                (if (.hasNext it)
                                                  (let [acc (f acc (.next it))]
                                                    (if (reduced? acc)
                                                      (reduced [@acc f])
                                                      (recur acc)))
                                                  [acc f])))  [partition-id]) 0))
                  [init f] (range (alength (.partitions rdd))))]
    (some-> v (vreset! f))
    acc))

(defn- update-proxied-fn [pf f]
  (if-some [v (some-> pf meta ::proxied-fn)]
    (do (vswap! v f) pf)
    (f pf)))

(extend-type org.apache.spark.api.java.JavaRDD
  CollReduce
  (coll-reduce
    ([rdd f]
      (reduce-rdd (update-proxied-fn f may-unmap-tuple2) (f) (.rdd rdd)))
    ([rdd f init]
      (reduce-rdd (update-proxied-fn f may-unmap-tuple2) init (.rdd rdd))))
  
  #_#_r/CollFold
  (coll-fold [rdd n combinef reducef]
     (-> rdd
       (.mapPartitions (reify org.apache.spark.api.java.function.FlatMapFunction
                         (call [_ it]
                           [(reduce reducef (combinef) (iterator-seq it))])))
       (.fold (combinef) (reify org.apache.spark.api.java.function.Function2
                           (call [_ a b] (combinef a b)))))))

;; unused
(extend-type org.apache.spark.api.java.JavaPairRDD
  CollReduce
  (coll-reduce
    ([rdd f]
      (reduce-rdd (update-proxied-fn f may-unmap-tuple2) (f) (.rdd rdd)))
    ([rdd f init]
      (reduce-rdd (update-proxied-fn f may-unmap-tuple2) init (.rdd rdd)))))

;; streaming
(declare ^:dynamic ^:powderkeg/no-sync ^org.apache.spark.streaming.api.java.JavaStreamingContext *ssc*)

(def ^:private ^:powderkeg/no-sync started-sscs (atom #{})) ; there should never be more than one...

(defmacro with-streaming-context [{:keys [duration ssc timeout checkpoint-dir]} & body]
  `(binding [*ssc* (-> ~(or ssc `(org.apache.spark.streaming.api.java.JavaStreamingContext. *sc* (org.apache.spark.streaming.Duration. ~duration)))
                     ~@(when checkpoint-dir [`(.checkpoint ~checkpoint-dir)]))]
     ~@body
     (.start *ssc*)
     (swap! started-sscs conj *ssc*)
     ~(if timeout 
       `(when (.awaitTerminationOrTimeout *ssc* ~timeout)
          (swap! started-sscs disj *ssc*))
       `(do
          (.awaitTermination *ssc*)
          (swap! started-sscs disj *ssc*)))))

(defprotocol DStreamable
  (-dstream [src ssc opts] "Creates a DStream, may be a noop if src is already a DStream and no options are provided."))

(extend-protocol DStreamable
  org.apache.spark.streaming.api.java.JavaDStream
  (-dstream [src ssc opts] src)
  String
  (-dstream [src ^org.apache.spark.streaming.api.java.JavaStreamingContext ssc opts] 
    (let [uri (java.net.URI. src)]
      (case (.getScheme uri)
        "tcp" (.socketTextStream ssc (.getHost uri) (.getPort uri))))))

(defn ^org.apache.spark.streaming.api.java.JavaDStream ensure-dstream
  "Converts (when needed) src to a dstream."
  ([src] (ensure-dstream src {}))
  ([src options]
    (-dstream src (:ssc options *ssc*) options)))

(defn ^org.apache.spark.streaming.api.java.JavaDStream dstream* [src xform options]
  (-> src 
    (ensure-dstream options)
    (.transform (reify org.apache.spark.api.java.function.Function
                  (call [_ a-rdd]
                    (rdd* a-rdd xform options))))))

(defn ^org.apache.spark.streaming.api.java.JavaDStream dstream [src & xforms-then-options]
  (let [[xforms {:as options}] (split-with (complement keyword?) xforms-then-options)
        xform (apply comp xforms)]
    (dstream* src xform options)))

(defn do-dstream*
  "Like dstream* but for forcing computation."
  [src xform options]
  (-> src 
    (ensure-dstream options)
    (.foreachRDD (reify org.apache.spark.api.java.function.Function
                   (call [_ a-rdd]
                     (do-rdd* a-rdd xform options))))))

(defmacro do-dstream
  "Forces computation and performs side-effects. Bindings are managed as per with-res."
  [src bindings & xforms-then-options]
  (let [[xforms {:as options}] (split-with (complement keyword?) xforms-then-options)]
    `(do-dstream* ~src
       (with-res ~bindings ~@xforms)
       ~options)))

;; todo: window

;; default logging
(when-not (-> (org.apache.log4j.LogManager/getRootLogger) .getAllAppenders .hasMoreElements)
  (binding [*out* *err*]
    (let [default "powderkeg/log4j_default.properties"]
      (when-some [url (io/resource default)]
        (org.apache.log4j.PropertyConfigurator/configure url)
        (println "Using Powderkeg's default logging profile:" default)))))

