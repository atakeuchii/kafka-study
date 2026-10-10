(defproject kafka-study "0.1.0-SNAPSHOT"
  :description "kafka study"
  :url "https://github.com/atakeuchii/kafka-study"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.5"]
                 [org.apache.kafka/kafka-clients "4.3.1"]
                 [org.slf4j/slf4j-simple "2.0.17"]]
  :jvm-opts ["-Dorg.slf4j.simpleLogger.defaultLogLevel=warn"]
  :repl-options {:init-ns kafka-study.core})
