(defproject fileape "0.10.4-SNAPSHOT"
  :description "Write data to files split by topic and rolled over on size or a timeout, files can be compressed using lzo, snappy or gzip"
  :url "https://github.com/gerritjvv/fileape"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}


  :global-vars {*warn-on-reflection* true
                *assert* true}

  :scm {:name "git"
        :url "https://github.com/gerritjvv/fileape.git"}

  :jvm-opts ["-Djava.library.path=/opt/hadoopgpl/native/Linux-amd64-64/"]
  :javac-options ["-target" "1.6" "-source" "1.6" "-Xlint:-options"]
  :java-source-paths ["java"]

  :dependencies [
                  ;[org.apache.hadoop/hadoop-common "2.2.0" :scope "provided"]
                  [fun-utils "0.6.1"]
                  [org.clojure/tools.logging "0.3.1"]
                  [log4j "1.2.16"]
                  [org.xerial.snappy/snappy-java "1.1.0"]
                  [org.clojure/core.async "0.2.374"]

                  [org.apache.parquet/parquet-common "1.8.1"]
                  [org.apache.parquet/parquet-encoding "1.8.1"]
                  [org.apache.parquet/parquet-column "1.8.1"]
                  [org.apache.parquet/parquet-hadoop "1.8.1"]

                  [org.apache.hadoop/hadoop-core "0.20.2" :exclusions [hsqldb]]
                  [org.clojure/clojure "1.6.0"]]

  :profiles {:dev {:dependencies [[midje "1.6.3"]
                                  [org.apache.parquet/parquet-tools "1.8.1"]

                                  [org.apache.parquet/parquet-hive-storage-handler "1.8.1"]
                                  [org.apache.hive/hive-serde "0.12.0"
                                   :exclusions [hive-exec
                                                org.codehaus.jackson/jackson-xc
                                                org.codehaus.jackson/jackson-jaxrs
                                                org.codehaus.jackson/jackson-core-asl
                                                com.twitter/parquet-hadoop-bundle
                                                org.mortbay.jetty/jetty-util
                                                org.mortbay.jetty/jetty
                                                javax.servlet/servlet-api
                                                commons-httpclient
                                                org.slf4j/slf4j-api]]]

                   :plugins [[lein-midje "3.0.1"]
                             [jonase/eastwood "0.2.1"]
                             [lein-codox "0.9.0"]]}}

  :repositories [["cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"]])
