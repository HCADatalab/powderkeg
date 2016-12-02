(ns powderkeg.pool
  "Functions in this namespace are meant to be used on the workers.")

(def ^:private lazy-threadpool (delay (java.util.concurrent.ScheduledThreadPoolExecutor. 0))) ; never force on the driver!

(defn- schedule [^Callable f ^long delay]
  (.schedule (-> ^java.util.concurrent.ScheduledThreadPoolExecutor @lazy-threadpool)
    f delay java.util.concurrent.TimeUnit/SECONDS))

(defn pool
  "Creates a pool."
  [create close keep-alive]
  (let [instances (atom [0 {}])]
    (fn
      ([] ; lease
        (let [[returns m :as v] @instances]
          (if-some [[x] (seq (vals m))]
            (if (compare-and-set! instances v [returns (dissoc m x)])
              x
              (recur))
            (create))))
      ([x] ; return
        (if (pos? keep-alive)
          (let [[t] (swap! instances (fn [[returns m]] (let [r (inc returns)] [r (assoc m x r)])))]
            (schedule 
              #(let [[returns m :as v] @instances]
                 (when (= t (m x))
                   (if (compare-and-set! instances v [returns (dissoc m x)])
                     (close x)
                     (recur))))
              keep-alive))
          (close x))))))

(defn share
  "Creates a shared resource."
  [create close keep-alive]
  (let [instance (atom [0 0 (delay (create))])]
    (fn
      ([] ; lease
        @(peek (swap! instance (fn [[refcount leases d]] [(inc refcount) (inc leases) d]))))
      ([x] ; return
        (let [[refcount leases :as v] (swap! instance (fn [[refcount leases d]] [(dec refcount) leases d]))]
          (when (zero? refcount)
            (if (pos? keep-alive)
              (schedule #(when (compare-and-set! instance v [0 0 (delay (create))])
                           (close x))
                keep-alive)
              (when (compare-and-set! instance v [0 0 (delay (create))])
                (close x)))))))))
