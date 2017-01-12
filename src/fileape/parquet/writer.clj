(ns
  ^{:doc
    "Support writing parquet files
     Usage:
       (def pf (open-parquet-file! k (parse-schema \"message test{ required binary name; repeated int32 age;}\") fname))
       (write! pf {\"name\" \"hi\" \"age\" [1 2 3]})
       (close! pf)

       "}
  fileape.parquet.writer
  (:require
    [clojure.string :as string]
    [clojure.tools.logging :refer [info]]
    [clojure.java.io :as io]
    [fileape.conf :as aconf]
    [fileape.parquet.write-support :as writer-support])
  (:import
    (org.apache.hadoop.fs FileSystem Path)
    (org.apache.hadoop.conf Configuration)
    (org.apache.parquet.schema MessageType MessageTypeParser)
    (org.apache.parquet.hadoop ParquetOutputFormat ParquetRecordWriter ParquetFileReader)
    (java.io File)
    (org.apache.parquet.hadoop.metadata CompressionCodecName ParquetMetadata)
    (org.apache.parquet.format.converter ParquetMetadataConverter ParquetMetadataConverter$NoFilter)))

;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Private functions

(defn as-path ^org.apache.hadoop.fs.Path [file]
  (cond
    (instance? org.apache.hadoop.fs.Path file) file
    (instance? File file) (org.apache.hadoop.fs.Path. (.getAbsolutePath ^File file))
    :else (org.apache.hadoop.fs.Path. (str file))))

(defn ^Configuration hadoop-conf [] (Configuration.))

(defn record-writer
  "Create a Parquet Record write that will accept Maps as groups and messages and primitive Java/Clojure types as values"
  [k conf ^MessageType schema ^File file ^CompressionCodecName codec]
  (info "record-writer with conf " conf)
  (let [hconf (doto
               (hadoop-conf)
               (.setLong "parquet.block.size" (aconf/get-conf k conf :parquet-block-size 52428800))
               (.setInt "parquet.page.size" (int (aconf/get-conf k conf :parquet-page-size 1048576)))
               (.setBoolean "parquet.enable.dictionary" (aconf/get-conf k conf :parquet-enable-dictionary false))
               (.setLong "parquet.memory.min.chunk.size" (aconf/get-conf k conf :parquet-memory-min-chunk-size 1048576)))

        path (.makeQualified (FileSystem/getLocal hconf) (Path. (.getAbsolutePath file)))
        output-format (ParquetOutputFormat. (writer-support/java-write-support schema {}))
        record-writer (.getRecordWriter output-format hconf path codec)]


    (info "parquet writer page.size " (ParquetOutputFormat/getPageSize hconf)
          " block.size " (.getLong hconf ParquetOutputFormat/BLOCK_SIZE 1048576)
          " memory.min.chunk.size " (.getLong hconf ParquetOutputFormat/MIN_MEMORY_ALLOCATION 1048576))
    record-writer))


;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Public functions

(defn ^MessageType parse-schema
  "Takes a schema string and parses it into a MessageType"
  [schema]
  (MessageTypeParser/parseMessageType (str schema)))

(defn open-parquet-file! [k ^MessageType type file & conf]
  (let [parquet-codec (aconf/get-conf k (apply array-map conf) :parquet-codec :gzip)

        file-obj (io/file file)]
    {:record-writer (record-writer k
                                   conf
                                   type file-obj
                                   (CompressionCodecName/valueOf CompressionCodecName (string/upper-case (name parquet-codec))))
     :file          file
     :type          type}))

(defn ^ParquetMetadata parquet-metadata
  "Returns the ParquetMetadata from a file
   file can be a string, File, or Hadoop Path.
   Can be used to validate files for correctness also.
   "
  [file]
  (ParquetFileReader/readFooter
    (hadoop-conf)
    (as-path file)
    ParquetMetadataConverter/NO_FILTER))

(defn parquet-ok?
  "Return nil when the parquet file is corrupt or cannot be read"
  [file]
  (try
    (parquet-metadata file)
    (catch Exception _ nil)))

(defn write! [{:keys [^ParquetRecordWriter record-writer]} msg]
  (locking record-writer
    (.write record-writer nil msg)))


(defn close! [{:keys [^ParquetRecordWriter record-writer]}]
  {:pre [record-writer]}
  (locking record-writer
    (.close record-writer nil)))

