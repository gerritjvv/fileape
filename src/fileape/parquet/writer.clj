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
  [conf ^MessageType schema ^File file ^CompressionCodecName codec]
  (info "record-writer with conf " conf)
  (let [conf (doto
               (hadoop-conf)
               (.setLong "parquet.block.size" (get conf :parquet-block-size 52428800))
               (.setInt "parquet.page.size" (int (get conf :parquet-page-size 1048576)))
               (.setBoolean "parquet.enable.dictionary" (get conf :parquet-enable-dictionary false))
               (.setLong "parquet.memory.min.chunk.size" (get conf :parquet-memory-min-chunk-size 1048576)))

        path (.makeQualified (FileSystem/getLocal conf) (Path. (.getAbsolutePath file)))
        output-format (ParquetOutputFormat. (writer-support/java-write-support schema {}))
        record-writer (.getRecordWriter output-format conf path codec)]


    (info "parquet writer page.size " (ParquetOutputFormat/getPageSize conf)
          " block.size " (.getLong conf ParquetOutputFormat/BLOCK_SIZE 1048576)
          " memory.min.chunk.size " (.getLong conf ParquetOutputFormat/MIN_MEMORY_ALLOCATION 1048576))
    record-writer))


;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Public functions

(defn ^MessageType parse-schema
  "Takes a schema string and parses it into a MessageType"
  [schema]
  (MessageTypeParser/parseMessageType (str schema)))

(defn open-parquet-file! [^MessageType type file & {:keys [parquet-codec] :or {parquet-codec :gzip} :as conf}]
  (let [file-obj (io/file file)]
    {:record-writer (record-writer conf
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


(defn close! [{:keys [^ParquetRecordWriter record-writer ^File file]}]
  {:pre [record-writer]}
  (locking record-writer
    (.close record-writer nil)
    (.)))

