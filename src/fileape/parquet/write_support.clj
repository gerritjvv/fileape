(ns
  ^{:doc "Implement java write support for parquet
          the different types and cases that can be written is
          done via multimethods while the actual WriterSupport is reified"}
  fileape.parquet.write-support
  (:require [fileape.util.lang :refer [case-enum]])
  (:import (org.apache.parquet.hadoop.api WriteSupport WriteSupport$WriteContext)
           (org.apache.parquet.schema MessageType GroupType Type$Repetition OriginalType Type PrimitiveType PrimitiveType$PrimitiveTypeName)
           (java.util Map Date)
           (org.apache.parquet.io.api RecordConsumer Binary)
           (java.util.concurrent TimeUnit)
           (fileape Util)))


(declare write-primitive-val)
(declare write-group-val)

;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; multi methods headers

(defmulti write-val
          "Write a value to parquet according to the provided schema and compatible with Hive,
           dispatched on originalType, if its a Map or List we write the Map or List out, otherwise
           we send to write-type-val which dispatches on type"
          (fn [rconsumer schema val] (.getOriginalType ^GroupType schema)))


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

(defn write-map-fields [^RecordConsumer rconsumer ^GroupType schema val]
  (let [field-count (.getFieldCount schema)
        ^Map val-map val]
    (dotimes [field field-count]
      (let [^Type field-type (.getType schema (int field))
            ^String field-name (.getName field-type)]
        (when-let [m-val (.get val-map field-name)]
          (when (Util/notNilOrEmpty m-val)
            (.startField rconsumer field-name (int field))
            (write-val   rconsumer field-type m-val)
            (.endField   rconsumer field-name (int field))))))))

(defn write-group-val [^RecordConsumer rconsumer ^GroupType schema val]
  ;;; either val is a List or a Map, in case of a List we need to
  ;;; read the values by index, in case of a Map read the values using get
  (.startGroup rconsumer)
  (if (instance? Map val)
    (write-map-fields rconsumer schema val)
    (let [field-count (.getFieldCount schema)
          val-vec (vec val)]
      (dotimes [field field-count]
        (let [^Type field-type (.getType schema (int field))
              ^String field-name (.getName field-type)]
          (when-let [v-val (nth val-vec field)]
            (.startField rconsumer field-name (int field))
            (write-val   rconsumer field-type v-val)
            (.endField   rconsumer field-name (int field)))))))
  (.endGroup rconsumer))


(defn _write-val-with-repetition [^RecordConsumer rconsumer ^Type schema val]
  (if (.isPrimitive schema)
    (reduce (fn [_ v]
              (write-primitive-val rconsumer (.asPrimitiveType schema) v)) nil val)
    (let [^GroupType group-schema (.asGroupType schema)
          field-count (.getFieldCount group-schema)]
      (dotimes [i field-count]
        (let [^Type sub-type (.getType group-schema (int i))]
          (.startField rconsumer (.getName sub-type) (int i))
          (reduce (fn [_ v]
                    (write-val rconsumer sub-type v)
                    nil) nil val)
          (.endField   rconsumer (.getName sub-type) (int i)))))))

(defn _write-val [rconsumer ^Type schema val]
  (if (.isPrimitive schema)
    (write-primitive-val rconsumer schema val)
    (write-group-val rconsumer schema val)))

(defn write-key-vals
  "Write the { required primtype key; optional type val; } part of a hive compatible map schema"
  [^RecordConsumer rconsumer ^GroupType sub-type val]
  (let [key-type (.getType sub-type (int 0))
        ^String key-name (.getName key-type)
        val-type (.getType sub-type (int 1))
        ^String val-name (.getName val-type)]

    (reduce-kv (fn [_ k v]
                 ;;start repeated group map
                 (.startGroup rconsumer)

                 (.startField rconsumer key-name (int 0))
                 (write-primitive-val rconsumer key-type k)
                 (.endField   rconsumer key-name (int 0))
                 (when v
                   (.startField rconsumer val-name (int 1))
                   (write-val rconsumer val-type v)
                   (.endField   rconsumer val-name (int 1)))
                 (.endGroup rconsumer))
               nil val)))

(defn date->int-seconds
  "Convert a date to seconds and return a casted int"
  [^Date date]
  (int (.toSeconds TimeUnit/MILLISECONDS (.getTime date))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; multi method impls


(defmethod write-val OriginalType/DATE [^RecordConsumer rconsumer ^Type schema val]
  ;;write hive compatible date
  ;;https://issues.apache.org/jira/secure/attachment/12696987/HIVE-8119.patch
  (prn "date :::::: " schema)
  (let [field-name (.getName schema)]
    (if (.isRepetition schema Type$Repetition/REPEATED)
      (reduce (fn [_ v]
                (write-primitive-val rconsumer (.asPrimitiveType schema) v)) nil (map date->int-seconds val))
      (write-primitive-val rconsumer schema (date->int-seconds val)))))

(defmethod write-val OriginalType/MAP [^RecordConsumer rconsumer ^GroupType schema val]
  ;;write a hive compatible map
  ;; optional group mymap (Map) { repeated group map (MapKeyVal) { required primtype key; optional type val; } }
  (let [field-count (.getFieldCount schema)]
    (dotimes [i field-count]
      (let [^GroupType sub-type (.asGroupType (.getType schema (int i)))
            sub-name (.getName sub-type)]

        ;;start field map
        (.startField rconsumer sub-name (int i))
        ;;write repeated fields for key val
        (write-key-vals rconsumer sub-type val)

        (.endField   rconsumer sub-name (int i))))))


(defmethod write-val OriginalType/LIST [^RecordConsumer rconsumer ^GroupType schema val]
  ;;write a hive compatible list
  ;; optional group mylist (LIST) { repeated group bag {optional type array_element;}}
  (let [field-count (.getFieldCount schema)]
    (dotimes [i field-count]
      (let [^GroupType sub-type (.asGroupType (.getType schema (int i)))
            ^String sub-name (.getName sub-type)
            ^Type array-element-type (.getType sub-type 0)
            ^String array-element-name (.getName array-element-type)]

        ;;start field bag
        (.startField rconsumer sub-name (int i))

        ;;write repeated bag of which the only field is array_element
        (reduce (fn [_ v]
                  ;;start repeated group bag
                  (.startGroup rconsumer)
                  (.startField rconsumer array-element-name (int 0))
                  (write-val rconsumer array-element-type v)
                  (.endField   rconsumer array-element-name (int 0))
                  (.endGroup rconsumer)
                  nil)
                nil val)

        (.endField   rconsumer sub-name (int i))))))



(defmethod write-val :default [^RecordConsumer rconsumer ^Type schema val]
  ;;; No original type is specified as Map List
  (if (.isRepetition schema Type$Repetition/REPEATED)
    (_write-val-with-repetition rconsumer schema val)
    (_write-val rconsumer schema val)))

;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; public functions


(defn java-write-support
  "Returns an instance of WriteSupport that will correctly
   serialise standard java types compatible with Hive"
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
          (write-map-fields rconsumer schema val)
          (.endMessage rconsumer))))))