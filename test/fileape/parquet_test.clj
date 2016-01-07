(ns
  ^{:doc
    "Test that we can write hive to parquet schemas using standard clojure/java data structures"}
  fileape.parquet-test


  (:require [fileape.parquet.parquet-util :as parquet-util]
            [clojure.test :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;; parquet schemas for tests

(defn test-schema-hive-1
  "Returns a parquet schema testing name:string info:struct<int,bigint> visitordates:array<string>, meta:map<string,string>"
  []
  (parquet-util/hive->parquet-schema {"name" "string" "info" "struct<age:int,id:bigint>" "visitordates" "array<string>" "meta" "map<string,string>"}))



(defn test-write-hive []
  (let [file (parquet-util/with-parquet-writer (test-schema-hive-1)
                                               {"name" "hi"
                                                "info" {"age" 1 "id" 1}
                                                "visitordates" ["1" "2" "3"]
                                                "meta" {"a" "a-2" "b" "b-3" "c" "c-4"}})]
    (parquet-util/read-hive-records file)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; Def Tests

;;tets we can write String, Struct, Array and Map for Hive
(deftest hive-write-test
  (is
    (=
      (vec (test-write-hive))
      [[[[["a" "a-2"] ["b" "b-3"] ["c" "c-4"]]] "hi" [1 1] [["1" "2" "3"]]]])))

