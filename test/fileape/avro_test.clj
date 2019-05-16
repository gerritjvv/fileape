(ns
  ^{:doc "Test avro writing"}
  fileape.avro-test
  (:require
    [fileape.avro.writer :as writer]
    [fileape.core :as file-ape]
    [midje.sweet :refer :all]
    [clojure.java.io :as io])
  (:import (java.util Iterator)
           (org.apache.avro Schema$Parser Schema)
           (io.confluent.kafka.schemaregistry.client MockSchemaRegistryClient)
           (org.apache.avro.generic IndexedRecord GenericData$Record GenericDatumReader)
           (org.apache.avro.file DataFileReader)
           (java.util.concurrent.atomic AtomicInteger)
           (java.io File)))



(defonce TEST-SCHEMA-STR "{\"type\":\"record\",\"name\":\"encryption_output\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"line\",\"type\":\"string\"}]}")

(defn ^Schema schema [json] (.parse (Schema$Parser.) (str json)))

(defonce ^String TOPIC "test")

(defn ^IndexedRecord record [^Schema schema]
  (GenericData$Record. schema))

(defn test-record [sc v]
  (let [^IndexedRecord r (record sc)]
    (.put r 0 (System/currentTimeMillis))
    (.put r 1 (str v))
    r))


(defn count-records [^Iterable reader]
  (let [counter (AtomicInteger. 0)
        ^Iterator it (.iterator reader)]
    (while (.hasNext it)
      (.next it)
      (.incrementAndGet counter))

    (.get counter)))

(defn list-files [d]
  (filter #(.isFile ^File %)
          (file-seq d)))

(fact "Test avro writing and reading"

      (let [
            schema1 (schema TEST-SCHEMA-STR)
            schema-client (doto
                            (MockSchemaRegistryClient.)
                            (.register TOPIC schema1))

            env (ref {})

            file (str "target/avrotest-file" (System/nanoTime))

            avro-writer (writer/open-avro-file! TOPIC file env {:schema-client schema-client :avro-schema-registry-url "http://localhost:8081"})

            l 10000]


        (dotimes [i l]
          (writer/write! avro-writer (test-record schema1 i)))

        (.flush (:data-writer avro-writer))
        (.fSync (:data-writer avro-writer))

        (writer/close! avro-writer)

        (Thread/sleep 10000)
        (count-records (DataFileReader/openReader (io/file (:file avro-writer))
                                                  (GenericDatumReader. ^Schema schema1))) => l))


(fact "Test avro writing via plugin"
      (let [schema1 (schema TEST-SCHEMA-STR)
            schema-client (doto
                            (MockSchemaRegistryClient.)
                            (.register TOPIC schema1))

            basedir (doto (File. (str "target/avrotest-file" (System/nanoTime)))
                      .mkdirs)

            ape (file-ape/ape {:codec                :avro
                               :avro-codec           :snappy
                               :rollover-abs-timeout 1000
                               :rollover-timeout     1000
                               :schema-client        schema-client
                               :base-dir             basedir})

            l 10000]

        (file-ape/write ape TOPIC (fn [{:keys [avro]}]
                                    (dotimes [i l]
                                      (writer/write! avro (test-record schema1 i)))))

        (file-ape/close ape)
        (reduce + (map  #(count-records (DataFileReader/openReader (io/file %)
                                                                   (GenericDatumReader. ^Schema schema1)))
                        (list-files basedir))) => l))
