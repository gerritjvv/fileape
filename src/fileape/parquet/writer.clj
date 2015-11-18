(ns
  ^{:doc
    "Support writing parquet files
     Usage:
       (def pf (open-parquet-file! (parse-schema \"message test{ required binary name; repeated int32 age;}\") fname))
       (write! pf {\"name\" \"hi\" \"age\" [1 2 3]})
       (close! pf)

       "}
  fileape.parquet.writer
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :refer [info]]
    [clojure.java.io :as io]
    [fileape.parquet.write-support :as writer-support])
  (:import
    (org.apache.hadoop.fs FileSystem Path)
    (org.apache.hadoop.conf Configuration)
    (org.apache.parquet.schema MessageType MessageTypeParser)
    (org.apache.parquet.hadoop ParquetOutputFormat ParquetRecordWriter)
    (java.io File)
    (org.apache.parquet.hadoop.metadata CompressionCodecName)))

;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Private functions

(defn record-writer
  "Create a Parquet Record write that will accept Maps as groups and messages and primitive Java/Clojure types as values"
  [conf ^MessageType schema ^File file ^CompressionCodecName codec]
  (info "record-writer with conf " conf)
  (let [conf (doto
               (Configuration.)
               (.setLong "parquet.block.size" (get conf :parquet-block-size 52428800))
               (.setLong "parquet.page.size" (get conf :parquet-page-size 1048576))
               (.setBoolean "parquet.enable.dictionary" (get conf :parquet-enable-dictionary true)))

        path (.makeQualified (FileSystem/getLocal conf) (Path. (.getAbsolutePath file)))
        output-format (ParquetOutputFormat. (writer-support/java-write-support schema {}))
        record-writer (.getRecordWriter output-format conf path codec)]

    record-writer))


;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Public functions

(defn ^MessageType parse-schema
  "Takes a schema string and parses it into a MessageType"
  [schema]
  (MessageTypeParser/parseMessageType (str schema)))

(defn open-parquet-file! [^MessageType type file & {:keys [parquet-codec] :or {parquet-codec :snappy} :as conf}]
  (let [file-obj (io/file file)]
    {:record-writer (record-writer conf
                                   type file-obj
                                   (CompressionCodecName/valueOf CompressionCodecName (string/upper-case (name parquet-codec))))
     :file          file
     :type          type}))


(defn write! [{:keys [^ParquetRecordWriter record-writer]} msg]
  (.write record-writer nil msg))


(defn close! [{:keys [^ParquetRecordWriter record-writer]}]
  {:pre [record-writer]}
  (.close record-writer nil))


(defn test-schema []
  (parse-schema "message AddressBook {
                                                           required binary owner;
                                                           optional group content_categories (LIST) {
                                                              repeated group bag {
                                                                optional group array_element {
                                                                    optional int64 id;
                                                                    optional double price;
                                                                    optional binary pricingtype;
                                                                    optional double price_usd;
                                                                 }
                                                              }
                                                           }

                                      }"))



(defn test-file []
  (let [fname "/tmp/testp"
        f (io/file fname)]
    (when (.exists f)
      (.delete f))
    (open-parquet-file! (parse-schema "message AddressBook {
                                          optional group categories { repeated group bag { optional int64 array_element; }}
                                          optional group deals { repeated group bag { optional group array_element { optional int64 id; optional double price; } }}\n
                                      }")
                        fname)))


(defn test-file2 []
  (let [fname "/tmp/testp"
        f (io/file fname)]
    (when (.exists f)
      (.delete f))
    (open-parquet-file! (test-schema)
                        fname)))