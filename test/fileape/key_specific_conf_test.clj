(ns
  ^{:doc "Test that we can configure different keys with different configuration options

         Options considered
          :base-dir
          :codec
          :rollover-size
          :rollover-timeout
          :rollover-abs-timeout
         "}
  fileape.key-specific-conf-test

  (:require [midje.sweet :refer :all]
            [fileape.core :as fape])
  (:import (java.io File)
           (java.io DataOutputStream)))


(defn app [f args]
  (doseq [arg args]
    (f arg)))

(defn ^File unique-base-dir []
  (doto
    (File. (str "target/tests/key-specific-conf-test/" (System/nanoTime)))
    .mkdirs))

(defn write-test-data [ape k]
  (dotimes [_ 100]
    (fape/write ape k (fn [{:keys [^DataOutputStream out]}]
                        (.write out (.getBytes (str "test-" (System/nanoTime) "\n") "UTF-8"))))))

(defn file->key [^File file]
  (first (clojure.string/split (.getName file) #"\.")))

(defn list-keyed-files
  "return a map of {:<key> [files]}
   where key is take from the file name as the first item before the first dot e.g a.myfile key == a"
  [dir]
  (let [files (filter #(.isFile %) (file-seq (clojure.java.io/file dir)))]
    (group-by file->key files)))

(facts "Test topic specific base-dir"
       (let [bdir-default (unique-base-dir)

             bdir1 (unique-base-dir)
             bdir2 (unique-base-dir)

             ape (fape/ape {:a.base-dir (.getAbsolutePath bdir1)
                            :a.codec    :snappy

                            :b.base-dir (.getAbsolutePath bdir2)
                            :b.codec    :bzip2

                            :base-dir   (.getAbsolutePath bdir-default)
                            :codec      :none})]

         (app (partial write-test-data ape) [:a :b :other])

         (app #(prn "basedir " %) [bdir1 bdir2 bdir-default])
         (fape/close ape)

         (app (fn [[k dir]]
                (let [m (list-keyed-files dir)
                      ks (keys m)]
                  (count ks) => 1
                  (first ks) => (name k)))

              [[:a bdir1] [:b bdir2]])))


(fact "Test topic specific services rollover timeout"
      (let [base-dir (unique-base-dir)

            ape2 (fape/ape {:codec :gzip :base-dir base-dir :check-freq 1000 :rollover-timeout 20000 :a.rollover-timeout 200})]

        (doall (map #(clojure.java.io/delete-file % :silently) (file-seq base-dir)))

        (fape/write ape2 :a (fn [{:keys [^DataOutputStream out]}] (.writeInt out (int 1))))
        (Thread/sleep 2000)
        ;we expect a file here
        (let [file (first (filter (fn [^File file] (re-find #"a" (.getName file))) (.listFiles base-dir)))]
          (prn ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> file " file " list files " (map #(.getName %) (.listFiles base-dir)))
          (nil? file) => false
          (.exists file) => true)

        (fape/close ape2)))


