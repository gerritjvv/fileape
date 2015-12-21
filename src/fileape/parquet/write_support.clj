(ns
  ^{:doc "Implement java write support for parquet
          the different types and cases that can be written is
          done via multimethods while the actual WriterSupport is reified"}
  fileape.parquet.write-support
  (:require [fileape.util.lang :refer [case-enum]]
            [clojure.tools.logging :refer [error info]])
  (:import (org.apache.parquet.hadoop.api WriteSupport WriteSupport$WriteContext)
           (org.apache.parquet.schema MessageType GroupType Type$Repetition OriginalType Type PrimitiveType PrimitiveType$PrimitiveTypeName)
           (java.util Map Date List)
           (org.apache.parquet.io.api RecordConsumer Binary)
           (java.util.concurrent TimeUnit)
           (fileape Util)
           (clojure.lang ISeq)))


(declare write-primitive-val)
(declare write-val)

;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; private functions and default multi method impls


(def BYTE-ARRAY-CLASS (Class/forName "[B"))

(defn asbinary ^"[B" [v]
  (if (instance? BYTE-ARRAY-CLASS v)
    (Binary/fromConstantByteArray ^"[B" v)
    (Binary/fromString (str v))))


(defn write-primitive-val [^RecordConsumer rconsumer ^PrimitiveType schema val]
  (case-enum  (.getPrimitiveTypeName schema)
    PrimitiveType$PrimitiveTypeName/INT64  (.addLong rconsumer (long val))
    PrimitiveType$PrimitiveTypeName/INT32   (.addInteger rconsumer (int val))
    PrimitiveType$PrimitiveTypeName/BOOLEAN (.addBoolean rconsumer (boolean val))
    PrimitiveType$PrimitiveTypeName/BINARY  (.addBinary rconsumer (asbinary val))
    PrimitiveType$PrimitiveTypeName/FLOAT   (.addFloat rconsumer (float val))
    PrimitiveType$PrimitiveTypeName/DOUBLE  (.addDouble rconsumer (double val))))


(defn start-field [^RecordConsumer rconsumer ^String field-name ^long i]
  (.startField rconsumer field-name (int i)))

(defn end-field [^RecordConsumer rconsumer ^String field-name ^long i]
  (.endField rconsumer field-name (int i)))

(defn start-group [^RecordConsumer rconsumer]
  (.startGroup rconsumer))
(defn end-group [^RecordConsumer rconsumer] (.endGroup rconsumer))


(defn date->int-seconds
  "Convert a date to seconds and return a casted int"
  [^Date date]
  (int (.toSeconds TimeUnit/MILLISECONDS (.getTime date))))


(defmulti write-extended-val "Any object other than a Java primitive, the dispatch is based on the Type::originalType" (fn [rconsumer ^Type schema val] (.getOriginalType schema)))

;;;;;;; optional group mylist (LIST) { repeated group bag {optional type array_element;}}
(defmethod write-extended-val OriginalType/LIST [rconsumer ^Type schema val]
  (when-not (or (instance? List val) (instance? ISeq val))
    (throw (RuntimeException. (str "Lists must be of type List or ISeq but got " val))))

  (let [^Type type (.getType (.asGroupType (.getType (.asGroupType schema) (int 0))) (int 0)) ]
        (start-group rconsumer)
        (start-field rconsumer "bag" 0)

        (reduce (fn [_ v]
                  (start-group rconsumer)
                  (when (Util/notNilOrEmpty v)
                    (start-field rconsumer "array_element" 0)
                    (write-val rconsumer type v)
                    (end-field rconsumer "array_element" 0))
                  (end-group rconsumer))
                nil
                val)

        (end-field rconsumer "bag" 0)
        (end-group rconsumer)))

(defmethod write-extended-val OriginalType/DATE [rconsumer schema val]
  ;;write hive compatible date
  ;;https://issues.apache.org/jira/secure/attachment/12696987/HIVE-8119.patch
  (write-primitive-val rconsumer PrimitiveType$PrimitiveTypeName/INT32 (date->int-seconds val)))


;;Default is a Group and the val type must be a Map
(defmethod write-extended-val :default [rconsumer ^Type schema val]
  (if-not (instance? Map val)
    (throw (RuntimeException. (str "Val must be a map but got " val))))

  (let [^GroupType type (.asGroupType schema)
        cnt (.getFieldCount type)]
        (start-group  rconsumer)
        (loop [i 0]
          (when (< i cnt)
            (let [^Type field (.getType type (int i))
                  field-name (.getName field)
                  map-v (get val field-name)]
              (when (Util/notNilOrEmpty map-v)
                (start-field rconsumer field-name i)
                (write-val rconsumer field map-v)
                (end-field rconsumer field-name i))
              (recur (inc i)))))
        (end-group rconsumer)))


(defn write-val
  "Write a primitive or extend (List Map) value"
  [rconsumer ^Type schema val]
  (if (.isPrimitive schema)
    (write-primitive-val rconsumer schema val)
    (write-extended-val rconsumer schema val)))

(defn write-message-fields
  "Write the fields of a top level message"
  [rconsumer ^GroupType schema ^Map val]
  (let [cnt (.getFieldCount schema)]
    (loop [i 0]
      (when (< i cnt)
        (let [^Type field (.getType schema (int i))
              field-name (.getName field)
              map-v (get val field-name)]
          (when (Util/notNilOrEmpty map-v)
            (start-field rconsumer field-name i)
            (write-val rconsumer field map-v)
            (end-field rconsumer field-name i))
          (recur (inc i)))))))

;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; public functions

(defn java-write-support
  "Returns an instance of WriteSupport that will correctly
   serialise standard java types compatible with Hive.

   Note: Repeated must have List as the original Type and will be a list type
         groups must be a Map type"
  [^MessageType schema meta]
  {:pre [(instance? Map meta) schema]}

  (let [record-consumer-state (atom nil)]

    (proxy [WriteSupport] []
      (init [_]
        (WriteSupport$WriteContext. schema ^Map meta))
      (prepareForWrite [record-consumer]
        (reset! record-consumer-state record-consumer))
      (write [val]
        (let [^RecordConsumer rconsumer @record-consumer-state]
          (.startMessage rconsumer)
          (write-message-fields rconsumer schema val)
          (.endMessage rconsumer))))))