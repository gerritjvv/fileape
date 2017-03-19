(ns
  ^{:doc "Provide protocols to allow extension for different storage and compression architectures
         e.g OutputStream gzip, lzo and partquet"}
  fileape.io-plugin
  (:require [fileape.util.io-util :as io-util]
            [fileape.conf :as aconf]
            [fileape.native-gzip :as native-gzip]
            [fileape.bzip2 :as bzip2]
            [fileape.avro.writer :as avro-writer]
            [clojure.string :as string])
  (:import (java.io OutputStream)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; Multimethos and protocols

(defmulti create-plugin "Create a plugin context returning a map depending on the codec key " (fn [k file env conf] (aconf/get-conf k conf :codec)))

(defmulti create-extension "Returns a string extesion based on the codec " (fn [codec] codec))

(defmulti close-plugin "Close a plugin context depending on the codec key " (fn [plugin-ctx] (:codec plugin-ctx)))

(defmulti update-env! "updates the environment for the plugin " (fn [k env conf & args] (aconf/get-conf k conf :codec)))

(defmulti validate-env! "validates that the current environment is valid for the plugin " (fn [k env conf] (aconf/get-conf k conf :codec)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Private Defaults and specific implementations for multi methods

(defn parse-key-remove-dates
  "For cache lookups keys with dates should be parsed to remove the date value
   as this is almost always not wanted e.g mylog-[date] for a schema lookup in avro should be mylog"
  [k]
  (string/replace k #"[\-0-9]{2,3}" ""))

(defmethod create-plugin :parquet [k file env conf]
  {:parquet (io-util/open-parquet-file! k file env conf)})

(defmethod create-plugin :avro [k file env {:keys [env-key-parser] :or {env-key-parser parse-key-remove-dates} :as conf}]
  {:avro (avro-writer/open-avro-file! (env-key-parser k) file env conf)})

(defmethod create-plugin :default [k file _ conf]
  (let [codec (aconf/get-conf k conf :codec :gzip)
        out-buffer-size (aconf/get-conf k conf :out-buffer-size 32000)
        use-buffer (aconf/get-conf k conf :use-buffer false)]
    (cond
      (= codec :gzip)
      (let [zipout (if use-buffer (io-util/create-buffered-zip-out file out-buffer-size) (io-util/create-zip-out file))]
        {:out zipout})
      (= codec :native-gzip)
      {:out (native-gzip/create-native-gzip file)}
      (= codec :bzip2)
      {:out (bzip2/create-bzip2 file)}
      (= codec :snappy)
      {:out (if use-buffer (io-util/create-buffered-snappy-out file out-buffer-size) (io-util/create-snappy-out file))}
      (= codec :none)
      {:out (if use-buffer (io-util/create-buffered-out file out-buffer-size) (io-util/create-out file))}
      :else
      (throw (RuntimeException. (str "The codec " codec " is not supported yet"))))))


(defmethod create-extension :default [codec]
  (cond
    (= codec :gzip) ".gz"
    (= codec :native-gzip) ".gz"
    (= codec :snappy) ".snz"
    (= codec :bzip2) ".bz2"
    (= codec :none) ".none"
    (= codec :parquet) ".parquet"
    (= codec :avro) ".avro"
    :else
    (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))


(defmethod close-plugin :parquet [{:keys [parquet]}]
  (when parquet
    (io-util/close-parquet-file! parquet)))

(defmethod close-plugin :avro [{:keys [avro]}]
  (when avro
    (avro-writer/close! avro)))

(defmethod close-plugin :default [{:keys [out]}]
  (when out
    (doto ^OutputStream out .flush .close)))


(defmethod update-env! :parquet [k env _ & {:keys [parquet-codec message-type]}]
  (let [entry {:parquet-codec parquet-codec :message-type message-type}]
    (io-util/validate-parquet-conf! k env entry)
    (dosync
      (alter env assoc k entry))))

(defmethod update-env! :default [& _])


(defmethod validate-env! :parquet [k env {:keys [env-key-parser] :or {env-key-parser parse-key-remove-dates}}]
  (let [env-key (env-key-parser k)]
    (io-util/validate-parquet-conf! env-key @env (get @env env-key))))

(defmethod validate-env! :default [& _] true)