(defproject hcadatalab/powderkeg "0.4.7"
  :description "Live-coding Spark clusters!"
  :url "https://github.com/HCADatalab/powderkeg"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  
  :profiles {:spark2 [^{:pom-scope :provided}
                      {:dependencies [[org.apache.spark/spark-core_2.11 "2.1.0"]
                                      [org.apache.spark/spark-streaming_2.11 "2.1.0"]]}
                      {:dependendies [[com.twitter/chill_2.11 "0.5.0" ; for carbonite
                                       :exclusions [org.scala-lang/scala-library]]]}]
             :spark1.5 [^{:pom-scope :provided}
                        {:dependencies [[org.apache.spark/spark-core_2.10 "1.5.2"]
                                        [org.apache.spark/spark-streaming_2.10 "1.5.2"]]}
                        {:dependendies [[com.twitter/chill_2.10 "0.5.0" ; for carbonite
                                         :exclusions [org.scala-lang/scala-library]]]}]}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 
                 [net.cgrand/xforms "0.5.1"]
                 
                 [com.esotericsoftware/kryo-shaded "4.0.0"]
                 [com.twitter/carbonite "1.4.0"
                  :exclusions [com.twitter/chill-java]]
                 [com.cemerick/pomegranate "0.3.0"]]
  :aot [powderkeg.repl]
  :java-source-paths ["src/main/java"]
  :source-paths ["src/main/clojure"])
