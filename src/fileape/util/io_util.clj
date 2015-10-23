(ns
  ^{:doc "Random io util functions for the io and io-plugin namespaces"}

  fileape.util.io-util
  (:require [fileape.parquet.writer :as parquet-writer])
  (:import (java.io DataOutputStream FileOutputStream BufferedOutputStream File)
           (java.util.zip GZIPOutputStream)
           (org.xerial.snappy SnappyOutputStream)
           (org.apache.parquet.schema MessageType)))



(defn create-zip-out [file]
  (DataOutputStream. (GZIPOutputStream. (FileOutputStream. file))))

(defn create-buffered-zip-out [file buffer-size]
  (DataOutputStream. (GZIPOutputStream. (BufferedOutputStream. (FileOutputStream. file) (int buffer-size)))))

(defn create-snappy-out [file]
  (DataOutputStream. (SnappyOutputStream. (FileOutputStream. file))))

(defn create-buffered-snappy-out [file buffer-size]
  (DataOutputStream. (SnappyOutputStream. (BufferedOutputStream. (FileOutputStream. file) (int buffer-size)))))

(defn create-out [file]
  (DataOutputStream. (FileOutputStream. file)))

(defn create-buffered-out [file buffer-size]
  (DataOutputStream. (BufferedOutputStream. (FileOutputStream. file) buffer-size)))

(defn validate-parquet-conf! [{:keys [parquet-codec message-type]}]
  (when-not
    (and parquet-codec
         message-type
         (instance? MessageType message-type))
    (throw (RuntimeException. (str "Parquet config must have parquet-codec, and message-type:MessageType but got " parquet-codec " " message-type)))))

(defn open-parquet-file! [k file env conf]
  (let [{:keys [parquet-codec message-type] :as entry}  (get @env k)]
    (validate-parquet-conf! entry)
    ;delete the file because the parquet open file will create it and fail if the file exists
    (.delete ^File (clojure.java.io/file file))
    (parquet-writer/open-parquet-file! message-type file :codec parquet-codec)))

(defn close-parquet-file! [parquet]
  (parquet-writer/close! parquet))