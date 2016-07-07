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
    (org.apache.parquet.hadoop ParquetOutputFormat ParquetRecordWriter ParquetFileWriter CodecFactory MemoryManager CodecFactory$BytesCompressor)
    (java.io File)
    (org.apache.parquet.hadoop.metadata CompressionCodecName)
    (org.apache.parquet.column ParquetProperties$WriterVersion)
    (java.lang.reflect Method)))

;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Private functions

;;; default codec factory created via reflection
(defonce ^CodecFactory codec-factory (.newInstance
                                       (doto
                                         (.getConstructor CodecFactory (into-array Class [Configuration]))
                                         (.setAccessible true))
                                       (into-array [(Configuration. true)])))

;;get the get compressor method from the private CodecFactory
(defonce ^Method codec-factory-get-compressor-method (doto
                                                       (.getMethod CodecFactory "getCompressor" (into-array Class [CompressionCodecName Integer/TYPE]))
                                                       (.setAccessible true)))

;;; default memory manager, all record writers will be attached to the same memory managner
(defonce ^MemoryManager memory-manager (MemoryManager. (float 0.95) 1048576))

;;; use reflection to get a compressor from the private CodecFactory
(defn get-compressor [^CodecFactory codec-factory codec page-size]
  (.invoke
    codec-factory-get-compressor-method
    codec-factory
    (into-array Object [codec (int page-size)])))


(defn record-writer
  "Create a Parquet Record write that will accept Maps as groups and messages and primitive Java/Clojure types as values"
  [conf ^MessageType schema ^File file ^CompressionCodecName codec]
  (info "record-writer with conf " conf)
  (let [
        block-size 52428800
        page-size 1048576
        conf (doto
               (Configuration.)
               (.setLong "parquet.block.size" (get conf :parquet-block-size block-size))
               (.setInt "parquet.page.size" (int (get conf :parquet-page-size page-size)))
               (.setBoolean "parquet.enable.dictionary" (get conf :parquet-enable-dictionary false))
               (.setLong "parquet.memory.min.chunk.size" (get conf :parquet-memory-min-chunk-size 1048576)))

        path (.makeQualified (FileSystem/getLocal conf) (Path. (.getAbsolutePath file)))

        ;;(Configuration configuration, MessageType schema, Path file)
        parquet-file-writer (doto
                              (ParquetFileWriter. conf schema path)
                              .start)

        ^CodecFactory$BytesCompressor bytes-compressor (get-compressor codec-factory codec (int page-size))

        ;;ParquetRecordWriter(ParquetFileWriter w, WriteSupport<T> writeSupport, MessageType schema, Map<String, String> extraMetaData, long blockSize, int pageSize, BytesCompressor compressor,
        ;; int dictionaryPageSize, boolean enableDictionary, boolean validating, WriterVersion writerVersion, MemoryManager memoryManager)
        parquet-record-writer (ParquetRecordWriter. parquet-file-writer
                                                    (writer-support/java-write-support schema {})
                                                    schema
                                                    {}
                                                    (long block-size)
                                                    (int page-size)
                                                    bytes-compressor
                                                    (int page-size)
                                                    false
                                                    false
                                                    ParquetProperties$WriterVersion/PARQUET_2_0
                                                    memory-manager)]


    (info "parquet writer page.size " (ParquetOutputFormat/getPageSize conf)
          " block.size " (.getLong conf ParquetOutputFormat/BLOCK_SIZE 1048576)
          " memory.min.chunk.size " (.getLong conf ParquetOutputFormat/MIN_MEMORY_ALLOCATION 1048576))
    {:record-writer parquet-record-writer
     :bytes-compressor bytes-compressor}))


;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Public functions

(defn ^MessageType parse-schema
  "Takes a schema string and parses it into a MessageType"
  [schema]
  (MessageTypeParser/parseMessageType (str schema)))

(defn open-parquet-file! [^MessageType type file & {:keys [parquet-codec] :or {parquet-codec :gzip} :as conf}]
  (let [file-obj (io/file file)
        {:keys [record-writer
                bytes-compressor]} (record-writer conf
                                                  type file-obj
                                                  (CompressionCodecName/valueOf CompressionCodecName (string/upper-case (name parquet-codec))))]
    {:record-writer record-writer
     :bytes-compressor bytes-compressor
     :file          file
     :type          type}))


(defn write! [{:keys [^ParquetRecordWriter record-writer]} msg]
  (locking record-writer
    (.write record-writer nil msg)))


(defn close! [{:keys [^ParquetRecordWriter record-writer]}]
  {:pre [record-writer]}
  (locking record-writer
    (.close record-writer nil)))

