(defproject fileape "0.3.0-SNAPSHOT"
  :description "WRite data to files split by topic and rolled over on size or a timeout, files can be compressed using lzo, snappy or gzip"
  :url "https://github.com/gerritjvv/fileape"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"] 
         [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]]
  :global-vars {*warn-on-reflection* true
                *assert* false}

  :dependencies [
                  [fun-utils "0.3.1"]
                  [org.clojure/tools.logging "0.2.3"]
                  [org.iq80.snappy/snappy "0.3"]
                  [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                  [midje "1.6-alpha2" :scope "test"]
                  [org.apache.hadoop/hadoop-core "0.20.2" :scope "provided" :exclusions [hsqldb]]
                  [org.clojure/clojure "1.5.1"]])
