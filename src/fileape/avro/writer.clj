(ns fileape.avro.writer
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer [warn]])
  (:import (io.confluent.kafka.schemaregistry.client CachedSchemaRegistryClient SchemaRegistryClient SchemaMetadata)
           (org.apache.avro Schema AvroRuntimeException)
           (org.apache.avro.generic GenericDatumWriter)
           (org.apache.avro.file DataFileWriter CodecFactory)
           (java.io File)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; utility functions

(defn ^CodecFactory valid-codec [codec-name]
  (try
    (CodecFactory/fromString (name (or codec-name "deflate")))
    (catch AvroRuntimeException _ (do
                                    (warn "invalid codec " codec-name " using deflate")
                                    (CodecFactory/fromString "deflate")))))

(defn ^DataFileWriter create-data-writer [file ^Schema schema avro-codec]
  (doto
    (DataFileWriter. (GenericDatumWriter. schema))
    (.setCodec (valid-codec avro-codec))
    (.create schema ^File (io/file file))))

(defn create-client-if-not-exist [env avro-schema-registry-url]
  (if (:schema-client env)
    (:schema-client env)
    (let [schema-client (CachedSchemaRegistryClient. (str avro-schema-registry-url) (int 1000))]
      (dosync
        (alter env assoc :schema-client schema-client))
      schema-client)))

(defn -load-schema
  "Allow schema-client to be passed in, otherwise create one using the avro-schema-registry-url"
  [k env {:keys [avro-schema-registry-url schema-client]}]
  {:pre [(or avro-schema-registry-url schema-client) env k]}
  (let [^SchemaRegistryClient client (if schema-client

                                       schema-client

                                       (create-client-if-not-exist env avro-schema-registry-url))

        ^SchemaMetadata metadata (.getLatestSchemaMetadata client (str k))
        schema (.getByID client (.getId metadata))]

    schema))

(def load-schema (memoize -load-schema))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; public functions

(defn open-avro-file!
  "If schema-client is added to conf, it will be used instead of creating a schema client"
  [k file env conf]
  {:pre [(or (:avro-schema-registry-url conf)
             (:schema-client conf))]}

  (let [avro-codec (:avro-codec conf)
        schema (load-schema k env conf)]

    {:data-writer (create-data-writer file schema avro-codec)
     :schema      schema
     :file        file
     :type        type}))


(defn write!
  "
   datum: should be an avro record
  "
  [{:keys [^DataFileWriter data-writer]} datum]
  (.append data-writer datum))

(defn close! [{:keys [^DataFileWriter data-writer]}]
  (.close data-writer))