(defproject hcadatalab/powderkeg "0.4.1"
  :description "Live-coding Spark clusters!"
  :url "https://github.com/HCADatalab/powderkeg"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  
  :profiles {:provided {:dependencies [[org.apache.spark/spark-core_2.10 "1.5.2"]
                                  [org.apache.spark/spark-streaming_2.10 "1.5.2"]]}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 
                 [net.cgrand/xforms "0.5.1"]
                 
                 [com.esotericsoftware/kryo-shaded "3.0.3"]
                 [com.twitter/carbonite "1.4.0"
                  :exclusions [com.twitter/chill-java]]
                 [com.twitter/chill_2.10 "0.5.0" ; for carbonite
                  :exclusions [org.scala-lang/scala-library]]
                 [com.cemerick/pomegranate "0.3.0"]]
  :aot [powderkeg.repl]
  :java-source-paths ["src/main/java"]
  :source-paths ["src/main/clojure"])
