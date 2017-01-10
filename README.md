# powderkeg

Live-coding the cluster!

## Usage

First, add Powderkeg and Spark to your dependencies:

```clj
:dependencies [[hcadatalab/powderkeg "0.4.1"]
               [org.apache.spark/spark-core_2.10 "1.5.2"]
               [org.apache.spark/spark-streaming_2.10 "1.5.2"]]
```

Then launch a repl (eg `lein repl`):

```clj
(require '[powderkeg.core :as keg])

(keg/connect! "spark://macbook-pro.home:7077") ; change uri, "local[2]" can do but that's no fun...

; sample lifted from sparkling
(into [] ; no collect, plain Clojure
  (keg/rdd ["This is a firest line"  ; here we provide data from a clojure collection.
            "Testing spark"
            "and powderkeg"
            "Happy hacking!"]
   (filter #(.contains % "spark")))) ; plain standard transducer, no new API
;=> ["Testing spark"]
```

## Goals

 * Good REPL even (and especially) against a real Spark cluster (as opposed to a local same-JVM node).
 * No AOT.
 * Leveraging Clojure. 
 * Not reimplementing every Spark method.

## Documentation

### Joining a cluster

Powderkeg had been tested against standalone and YARN clusters.

`(keg/connect! url)` and `(keg/disconnect!)` are meant only to be used at the repl.

You can also use `./spark-submit --master spark:... --class powderkeg.repl your.jar`.

Applications should use:

`./spark-submit --master spark:... --class powderkeg.repl your.jar your.ns/main-fn app-arg0 ...`.

Where `your.ns/main-fn` is a vararg function (which receives arguments as strings) and the entry point of the application. `*sc*` will be bound.

### Creating a RDD 
Currently one can only creates a RDD from a collection (including `nil`) or another RDD (either returned by a keg function or by good old interop).

To add a new kind of source for RDDs, one has to extend the `RDDable` protocol.

The main function of powderkeg is `rdd` which takes a source as first argument and zero or more transducers and then options.

```clj
(keg/rdd (range 100)     ; source
  (filter odd?)          ; 1st transducer to apply
  (map inc)              ; 2nd transducer
  :partitions 2)         ; and options
```

This returns a plain `org.apache.spark.api.java.JavaRDD` instance. No wrapper. At some point it may return a scala RDD.
Bypassing the Java api layer is under consideration but it wouldn't change the overall design. 

### Getting results

RDDs can be reduced so all you have to do is call `into` on them.

```clj
(into [] (keg/rdd (range 10)))
;=> [0 1 2 3 4 5 6 7 8 9]
``` 

You may encounter erroneous results with some stateful transducers. However if it's from a very narrow class of stateful transducers: stateful transducers which calls the underlying reducing function on completion (eg `partition-by`). Furthermore you usually don't want to perform expensive computation (and data transfer) on the driver, so most of the time the only transducers you want to use with `into` is `take`. Others tend to be best pushed into the RDD.

This being said if you really have to use a "flushing" transducer then you to work around the issue by either using `scomp` instead of `comp`
to create a transducer whose reducing functions states will be correctly passed around cluster nodes. Or by using `keg/into` which is really
just `into` with implicit `scomp`.

```clj
;; Both are safe
(into [] (keg/scomp (take 5)) (range 10))
(keg/into [] (take 5) (range 10))
;; in truth, take is not a problematic transducer and doesn't need to be protected.

;; partition-by is problematic.
(into [] (partition-by #(quot % 6)) (keg/rdd (range 20)))
=> [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17]]
(keg/into [] (partition-by #(quot % 6)) (keg/rdd (range 20)))
=> [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17] [18 19]]
(into [] (keg/scomp (partition-by #(quot % 6))) (keg/rdd (range 20)))
=> [[0 1 2 3 4 5] [6 7 8 9 10 11] [12 13 14 15 16 17] [18 19]]
```

`scomp` is still useful if you use `reduce`, `eduction` or `transduce` instead of `keg/into` but, again, think twice before doing that: *can't you push the computation in the RDD*?

### Pair RDDs

Pair RDDs are just RDDs that happen to contain pairs (`scala.Tuple2` to be exact).

Powderkeg automatically converts from/to map entries and 2-item vectors to/from tuple2. Furthermore this conversion
may be optimized away when using xforms transducers supporting (deconstructed) pairs.

So `(keg/rdd {:a 1 :b 2})` is a pair rdd.

`(keg/rdd (range 100) (map (fn [n] [(mod n 7) n])))` and `(keg/rdd (range 100) (x/for [n %] [(mod n 7) n]))` defines the same pair RDD but the second one doesn't allocate intermediate vectors.

However RDDs created with `rdd` are not partitioned. To partition a RDD you have to use `by-key` instead.

```clj
=> (keg/rdd (range 100) (x/for [n %] [(mod n 7) n]))
#object[org.apache.spark.api.java.JavaRDD 0x16a8a7f3 "MapPartitionsRDD[164] at mapPartitions at form-init6760256745287702501.clj:7"]
=> (-> *1 .partitioner .orNull)
nil
=> (keg/by-key *1)
#object[org.apache.spark.api.java.JavaRDD 0x29925883 "ShuffledRDD[167] at partitionBy at core.clj:232"]
=> (-> *1 .partitioner .orNull)
#object[org.apache.spark.HashPartitioner 0x637140f3 "org.apache.spark.HashPartitioner@2"]
```

When the input of `by-key` is not partitioned yet, it's automatically partitioned on the first item of the pair. Here partitioning means that data is shuffled accross the workers so that for a key, all its pairs are in the same partition (and thus on the same worker).

When the input is not already keyed – or the existing key is not the correct one — the `:key` option allows to specify a keying function:

```clj
=> (keg/by-key (range 100) :key #(mod % 7))
#object[org.apache.spark.api.java.JavaRDD 0x145f61ff "ShuffledRDD[174] at partitionBy at core.clj:232"]
=> (-> *1 .partitioner .orNull)
#object[org.apache.spark.HashPartitioner 0x6dc001ee "org.apache.spark.HashPartitioner@2"]
```

`key-by` can also be used to transform values (the pairs second items):

```clj
=> (into {}
     (keg/by-key (range 100) :key odd?
       (x/reduce +)))
{false 2450, true 2500}
```

However **when `by-key` shuffles data around, transformations are applied twice.** In the above example that's the case and it's a good thing: values are locally summed before being shuffled and summed again.
This works nicely because summing is associative. But more often than not the computation to perform is not associative and having it run twice will lead to either failure or, worst, erroneous results.

```clj
=> (into {}
     (keg/by-key {:a 1 :b 2}
       (map inc)))
{:b 4, :a 3} ; oops
;; this doesn't happen when the input is already partitioned:
=> (into {}
     (keg/by-key (keg/by-key {:a 1 :b 2})
       (map inc)
       :shuffle nil))
{:b 3, :a 2}
;; or when we disable partitioning (*)
=> (into {}
     (keg/by-key {:a 1 :b 2}
       (map inc)
       :shuffle nil))
{:a 2, :b 3}
;; or when we perform the computation before shuffling (*)
=> (into {}
     (keg/by-key {:a 1 :b 2}
       :pre (map inc)))
{:b 3, :a 2}
;; or after
=> (into {}
     (keg/by-key {:a 1 :b 2}
       :post (map inc)))
{:b 3, :a 2}
;; (*) if the transformation is an aggregation you may get erroneous results.
```

All these options may be combined
```clj
=> (into {}
     (keg/by-key (range 100) :key odd?
       :pre (map inc)
       (x/reduce +)
       :post (map str)))
{false "2500", true "2550"}
```

`by-key` options syntax is relaxed enough: anything preceded by a keyword is an option, else it's a transformation. That's why in the above example the transformation occur in the
middle of named options.


## TODO

Streams (in progress), broadcast helpers, dataframes...

## License

Copyright © 2015-2016 HCA

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
