(ns
  ^{:doc "Implement java write support for parquet
          the different types and cases that can be written is
          done via multimethods while the actual WriterSupport is reified"}
  fileape.parquet.write-support
  (:require [fileape.util.lang :refer [case-enum]]
            [clojure.tools.logging :refer [error info]])
  (:import (org.apache.parquet.hadoop.api WriteSupport WriteSupport$WriteContext)
           (org.apache.parquet.schema MessageType GroupType OriginalType Type PrimitiveType PrimitiveType$PrimitiveTypeName)
           (java.util Map Date List)
           (org.apache.parquet.io.api RecordConsumer Binary)
           (java.util.concurrent TimeUnit)
           (fileape Util)
           (clojure.lang ISeq)))


(declare write-primitive-val)
(declare write-val)
(declare write-message-fields)

;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; private functions and default multi method impls


(def BYTE-ARRAY-CLASS (Class/forName "[B"))

(defn asbinary ^"[B" [v]
  (if (instance? BYTE-ARRAY-CLASS v)
    (Binary/fromConstantByteArray ^"[B" v)
    (Binary/fromString (str v))))


(defn check-is-map
  "If v is not null then check that v is an instanceof java.util.Map, if not an exception is thrown"
  [v]
  (when v
    (if-not (instance? Map v)
      (throw (RuntimeException. (str "Val must be a map but got " v))))))

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


(defn get-map-schemas
  "The schema should be a valid hive map and have the format
   #<GroupType optional group [map-field-name] (MAP) {\n  repeated group map (MAP_KEY_VALUE) {\n    required binary key;\n    optional binary value;\n  }\n}"
  [^Type schema]
  (let [map-type (.asGroupType (.getType (.asGroupType schema) "map"))]
    [(.getType map-type "key") (.getType map-type "value")]))

(defn write-key-value
  "Write a hive map compatible data structure from the schema {\n  repeated group map (MAP_KEY_VALUE) {\n    required binary key;\n    optional binary value;\n  }\n}
   Note only the key value parts are written, the group and field for map needs to be created before and ended after this function is called for all key values"
  [rconsumer ^Type schema k v]
  (let [[^Type key-type ^Type val-type] (get-map-schemas schema)
        key-name (.getName key-type)
        val-name (.getName val-type)]

    (start-group rconsumer)
    (start-field rconsumer key-name 0)
    (write-val rconsumer key-type k)
    (end-field rconsumer key-name 0)

    (start-field rconsumer val-name 1)
    (write-val rconsumer val-type v)
    (end-field rconsumer val-name 1)

    (end-group rconsumer)))

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


(defmethod write-extended-val OriginalType/MAP [rconsumer ^Type schema val]
  ;;write a hive compatible map type
  ;;#<GroupType optional group [map-field-name] (MAP) {
  ;;  repeated group map (MAP_KEY_VALUE) {
  ;;                                      required binary key;
  ;;                                               optional binary value;
  ;;                                      }
  ;;}

  (check-is-map val)
  (start-field rconsumer (.getName schema) 0)

  ;;for each key val call write-key-value
  (reduce-kv #(write-key-value rconsumer schema %2 %3) nil val)

  (end-field rconsumer (.getName schema) 0))



;;Default is a Group and the val type must be a Map
(defmethod write-extended-val :default [rconsumer ^Type schema val]
  (check-is-map val)

  (start-group  rconsumer)
  (write-message-fields rconsumer (.asGroupType schema) val)
  (end-group rconsumer))

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