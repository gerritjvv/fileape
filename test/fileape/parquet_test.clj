(ns
  ^{:doc
    "Test that we can write hive to parquet schemas using standard clojure/java data structures"}
  fileape.parquet-test


  (:require [clojure.string :as string]
            [fileape.parquet.writer :as pwriter]
            [fileape.parquet.parquet-util :as parquet-util]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import
    (org.apache.hadoop.hive.serde2.typeinfo TypeInfo TypeInfoUtils)
    (org.apache.hadoop.hive.ql.io.parquet.convert HiveSchemaConverter)
    (org.apache.parquet.schema MessageType PrimitiveType Type$Repetition PrimitiveType$PrimitiveTypeName OriginalType)
    (java.util Date)
    (org.apache.parquet.tools.read SimpleReadSupport)
    (java.util.concurrent TimeUnit)
    (org.apache.hadoop.hive.ql.io.parquet.read DataWritableReadSupport)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; private test helper functions

(defn ^TypeInfo str->typeinfo [^String s] (vec (TypeInfoUtils/getTypeInfosFromTypeString (string/trim s))))


(defn vals-as-typeinfo [hive-type-map]
  (mapcat str->typeinfo (flatten (vals hive-type-map))))

(defn ^MessageType
hive->parquet-schema [hive-type-map]
  (HiveSchemaConverter/convert (keys hive-type-map) (vals-as-typeinfo hive-type-map)))

(defn dir->parquet-records [dir & {:keys [reader-support] :or {reader-support (DataWritableReadSupport.)}}]
  (parquet-util/record-seq (parquet-util/open-reader dir :reader-support reader-support)))


(defn with-parquet-writer
  "Open a parquet writer using the schema and write msgs to it
   Return the records using dir->parquet-records"
  [schema msg & {:keys [reader-support] :or {reader-support (DataWritableReadSupport.)}}]
  (let [test-dir (str "target/tests/parquet-test-" (System/nanoTime))
        file (io/file (str  test-dir "/myfile.parquet"))
        writer (pwriter/open-parquet-file! schema file :parquet-codec :uncompressed)]

    (pwriter/write! writer msg)
    (pwriter/close! writer)

    (dir->parquet-records test-dir :reader-support reader-support)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; parquet schemas for tests

(defn test-schema-hive-1
  "Returns a parquet schema testing name:string info:struct<int,bigint> visitordates:array<string>, meta:map<string,string>"
  []
  (hive->parquet-schema {"name" "string" "info" "struct<age:int,id:bigint>" "visitordates" "array<string>" "meta" "map<string,string>"}))


(defn test-schema-1
  "Returns a simple test schema with one group and one repeated primitive"
  []
  (pwriter/parse-schema
                            "message testschema1 {
                                optional int64 age;
                                optional binary name;
                                repeated group address{
                                    optional binary street;
                                    optional binary city;
                                }
                                repeated binary tel;
                            }"))


;; public PrimitiveType(Repetition repetition, PrimitiveType.PrimitiveTypeName primitive, String name, OriginalType originalType) {

(defn test-schema-date-1
  "Test write date, we can't yet use the hive libraries to test, so
   need to manually create the schema
   message mysg { optional int32 mydate (DATE) }"
  []
  (MessageType.
    "mymsg" [(PrimitiveType. Type$Repetition/OPTIONAL PrimitiveType$PrimitiveTypeName/INT32 "mydate" OriginalType/DATE)]))

(defn test-schema-date-2
  "Test write date, we can't yet use the hive libraries to test, so
   need to manually create the schema
   message mysg { repeated int32 mydate (DATE) }
   "
  []
  (MessageType. "mymsg" [(PrimitiveType. Type$Repetition/REPEATED PrimitiveType$PrimitiveTypeName/INT32 "mydate" OriginalType/DATE)]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;; test functions

(defn write-schema-hive1? []
  (= (with-parquet-writer (test-schema-hive-1)
                          {"name" "abc"
                           "info" {"age" 20 "id" 1000}
                           "visitordates" ["1" "2" "3"]
                           "meta" {"m1" 1 "m2" "3"}})
     '([[["m1" "1"] ["m2" "3"]]] "abc" [20 1000] [["1" "2" "3"]])))

(defn write-schema1?
  "Test that we can write a simple schema 1 with one binary name, a repeated group address and a repeated primitive"
  []
  ;;TODO fix double reading from SimpleRead Support
  ;; exterior analysis shows that the file is correct
  (=
    (with-parquet-writer (test-schema-1)
                         {"age" 20
                          "name" "ABC"
                          "address" [{"street" "st1" "city" "city1"} {"street" "st2" "city" "city2"}]
                          "tel" ["123" "456" "789"]}
                         :reader-support (SimpleReadSupport.))
    '(20 "ABC" ("{\"street\" \"st1\", \"city\" \"city1\"}" "{\"street\" \"st1\", \"city\" \"city1\"}") ("{\"street\" \"st2\", \"city\" \"city2\"}" "{\"street\" \"st2\", \"city\" \"city2\"}") "123" "456" "789")))


(defn write-date-schema1?
  "Test that we can write a java date using the hive schema to represent it as a int32"
  []
  (let [date (Date.)]
    (=
      (first
        (with-parquet-writer
          (test-schema-date-1)
          {"mydate" date}))
      (int (.toSeconds TimeUnit/MILLISECONDS (.getTime date))))))

(defn write-date-schema-2?
  "Test that we can write a java date using the hive schema to represent it as a repeated int32"
  []
  (let [date (Date.)]
    (=
      (first
        (with-parquet-writer
          (test-schema-date-2)
          {"mydate" [date date date]}))
      (int (.toSeconds TimeUnit/MILLISECONDS (.getTime date))))))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;; test hooks

(deftest write-schema-hive1-test []
         (is (write-schema-hive1?)))

(deftest write-schema1-test []
         (is (write-schema1?)))

(deftest write-date-schema1-test []
         (is (write-date-schema1?)))