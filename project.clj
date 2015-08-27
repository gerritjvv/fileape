(defproject fileape "0.7.7"
  :description "Write data to files split by topic and rolled over on size or a timeout, files can be compressed using lzo, snappy or gzip"
  :url "https://github.com/gerritjvv/fileape"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [
         [lein-rpm "0.0.5"] [lein-midje "3.0.1"] [lein-marginalia "0.7.1"] 
         [lein-kibit "0.0.8"] [no-man-is-an-island/lein-eclipse "2.0.0"]]

  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.0.1"]]}}

  :global-vars {*warn-on-reflection* true
                *assert* false}

  :scm {:name "git"
        :url "https://github.com/gerritjvv/fileape.git"}

  :jvm-opts ["-Djava.library.path=/opt/hadoopgpl/native/Linux-amd64-64/"]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :java-source-paths ["java"]
  :dependencies [
                  ;[org.apache.hadoop/hadoop-common "2.2.0" :scope "provided"]
                  [fun-utils "0.5.8-SNAPSHOT"]
                  [org.clojure/tools.logging "0.3.1"]
                  [log4j "1.2.16"]
                  [org.xerial.snappy/snappy-java "1.1.0"]
                  [org.clojure/core.async "0.1.267.0-0d7780-alpha"]
                  [midje "1.6.3" :scope "test"]
                  [org.apache.hadoop/hadoop-core "0.20.2" :exclusions [hsqldb]]
                  [org.clojure/clojure "1.5.1"]])
