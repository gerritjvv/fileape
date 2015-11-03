(ns
  ^{:doc "helper functions to read parquet files"}
  fileape.parquet.parquet-util
  (:import
    (org.apache.hadoop.hive.ql.io.parquet.read DataWritableReadSupport)
    (org.apache.parquet.hadoop ParquetReader)
    (org.apache.hadoop.fs Path)
    (org.apache.hadoop.io ArrayWritable IntWritable DoubleWritable BooleanWritable FloatWritable LongWritable Text)
    (org.apache.parquet.hadoop.api ReadSupport)
    (org.apache.parquet.tools.read SimpleRecord SimpleRecord$NameValue)))


(defprotocol IWritable
  (lift-writable [this]))

(defprotocol IGet
  (get-record-val [this]))


(extend-protocol IGet
  ArrayWritable
  (get-record-val [this] (.get ^ArrayWritable this))

  SimpleRecord
  (get-record-val [this] (.getValues ^SimpleRecord this)))

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

;org.apache.parquet.hadoop.api
(defn
  ^ParquetReader
  open-reader [^String file & {:keys [reader-support] :or {reader-support (DataWritableReadSupport.)}}]
  (.build (ParquetReader/builder ^ReadSupport reader-support (Path. file))))


(defn
  ^ArrayWritable
  next-record [^ParquetReader reader]
  (.read reader))

(defn record-seq
  "Return a sequence of sequence of records
   each ArrayWritable's real value is lifted out so that the primitive
   values are returned"
  [^ParquetReader reader]
  (if-let [record (next-record reader)]
    (concat (map lift-writable (get-record-val record))
          (lazy-seq
            (record-seq reader)))))
