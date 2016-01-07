(ns
  ^{:doc "helper functions to read parquet files"}
  fileape.parquet.parquet-util
  (:require [fileape.parquet.writer :as pwriter]
            [clojure.java.io :as io]
            [clojure.string :as string])
  (:import
    (org.apache.hadoop.hive.ql.io.parquet.read DataWritableReadSupport)
    (org.apache.parquet.hadoop ParquetReader ParquetRecordReader)
    (org.apache.hadoop.fs Path)
    (org.apache.hadoop.io ArrayWritable IntWritable DoubleWritable BooleanWritable FloatWritable LongWritable Text)
    (org.apache.parquet.hadoop.api ReadSupport)
    (org.apache.parquet.tools.read SimpleRecord SimpleRecord$NameValue)
    [org.apache.hadoop.hive.ql.io.parquet.writable BinaryWritable]
    [org.apache.hadoop.mapreduce.lib.input FileSplit]
    [java.io File]
    [org.apache.hadoop.hive.ql.io.parquet.convert HiveSchemaConverter]
    [org.apache.parquet.schema MessageType]
    [org.apache.parquet.filter2.compat FilterCompat]
    [org.apache.hadoop.mapreduce TaskAttemptContext TaskAttemptID TaskID]
    [org.apache.hadoop.conf Configuration]
    [org.apache.hadoop.hive.serde2.typeinfo TypeInfo TypeInfoUtils]))


(defprotocol IWritable
  (lift-writable [this]))

(extend-protocol IWritable
  IntWritable
  (lift-writable [^IntWritable this] (.get this))
  LongWritable
  (lift-writable [^LongWritable this] (.get this))
  DoubleWritable
  (lift-writable [^DoubleWritable this] (.get this))
  BooleanWritable
  (lift-writable [^BooleanWritable this] (.get this))
  FloatWritable
  (lift-writable [^FloatWritable this] (.get this))
  Text
  (lift-writable [^Text this]
    (str this))

  ArrayWritable
  (lift-writable [^ArrayWritable this]
    (mapv lift-writable (.get this)))

  nil
  (lift-writable [_] nil)

  org.apache.hadoop.hive.serde2.io.DoubleWritable
  (lift-writable [^org.apache.hadoop.hive.serde2.io.DoubleWritable this] (.get this))
  org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable$DicBinaryWritable
  (lift-writable [^org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable$DicBinaryWritable this]
    (.getString this))
  org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable
  (lift-writable [^org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable this]
    (String. (.getBytes this) "UTF-8"))

  SimpleRecord
  (lift-writable [^SimpleRecord this] (map lift-writable (.getValues this)))

  SimpleRecord$NameValue
  (lift-writable [^SimpleRecord$NameValue this]
    (lift-writable (.getValue this))))

(extend-protocol IWritable
  (Class/forName "[B")
  (lift-writable [this] (String. ^"[B" this))

  Number
  (lift-writable [this] this)

  String
  (lift-writable [this] this))


(defn ^TypeInfo str->typeinfo [^String s] (vec (TypeInfoUtils/getTypeInfosFromTypeString (string/trim s))))

(defn vals-as-typeinfo [hive-type-map]
  (mapcat str->typeinfo (flatten (vals hive-type-map))))

(defn ^MessageType
  hive->parquet-schema [hive-type-map]
  (HiveSchemaConverter/convert (keys hive-type-map) (vals-as-typeinfo hive-type-map)))


(defn ^FileSplit file-split [^File file]
  (FileSplit.
    (Path. (.getParent file) (.getName file))
    0
    (.length file)
    (into-array ["localhost"])))



(defn lazy-seq-hive-records [^ParquetRecordReader reader]
  (when (.nextKeyValue reader)
    (cons (lift-writable (.getCurrentValue reader))
          (lazy-seq-hive-records reader))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;; public functions

(defn with-parquet-writer
  "Open a parquet writer using the schema and write msgs to it
   Return the records using dir->parquet-records"
  [schema msg]
  (let [test-dir (str "target/tests/parquet-test-" (System/nanoTime))
        file (io/file (str  test-dir "/myfile.parquet"))
        writer (pwriter/open-parquet-file! schema file :parquet-codec :uncompressed)]

    (pwriter/write! writer msg)
    (pwriter/close! writer)

    file))

(defn read-hive-records [file]
  (let [record-reader (ParquetRecordReader.
                        (DataWritableReadSupport.)
                        FilterCompat/NOOP)]

    (.initialize record-reader
                 (file-split file)
                 (TaskAttemptContext. (Configuration.) (TaskAttemptID. (TaskID.) (int 0))))

    (lazy-seq-hive-records record-reader)))