(defproject hcadatalab/powderkeg "0.5.0"
  :description "Live-coding Spark clusters!"
  :url "https://github.com/HCADatalab/powderkeg"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aliases {"test-all" ["with-profile" "spark1.5:spark2" "test"]}
  :test-selectors {:default (complement :integration)
                   :integration :integration}
  :profiles {:default [:spark2]
             :spark2 [:leiningen/default :spark2-deps :kryo-4]
             :spark1.5 [:leiningen/default :spark1.5-deps :kryo-3]
             :spark2-deps ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.11 "2.1.0"]
                             [org.apache.spark/spark-streaming_2.11 "2.1.0"]]}
             :spark1.5-deps ^{:pom-scope :provided}
             {:dependencies [[org.apache.spark/spark-core_2.10 "1.5.2"]
                             [org.apache.spark/spark-streaming_2.10 "1.5.2"]]}
             :kryo-3 {:dependencies [[com.esotericsoftware/kryo-shaded "3.0.3"]]}
             :kryo-4 {:dependencies [[com.esotericsoftware/kryo-shaded "4.0.0"]]}}
  :dependencies [[org.clojure/clojure "1.9.0-alpha15"]
                 [net.cgrand/xforms "0.5.1"]
                 [com.twitter/carbonite "1.4.0" :exclusions [com.twitter/chill-java]]
                 [com.cemerick/pomegranate "0.3.0"]]
  :aot [powderkeg.repl]
  :java-source-paths ["src/main/java"]
  :source-paths ["src/main/clojure"]
  :target-path "target/%s/")
