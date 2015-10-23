(ns
  ^{:doc "Provide protocols to allow extension for different storage and compression architectures
         e.g OutputStream gzip, lzo and partquet"}
  fileape.io-plugin
  (:require [fileape.util.io-util :as io-util]
            [fileape.native-gzip :as native-gzip]
            [fileape.bzip2 :as bzip2])
  (:import (java.io OutputStream)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;; Multimethos and protocols

(defmulti create-plugin "Create a plugin context returning a map depending on the codec key " (fn [k file env conf] (:codec conf)))

(defmulti create-extension "Returns a string extesion based on the codec " (fn [codec] codec))

(defmulti close-plugin "Close a plugin context depending on the codec key " (fn [plugin-ctx] (:codec plugin-ctx)))

(defmulti update-env! "updates the environment for the plugin " (fn [k env conf & args] (:codec conf)))

(defmulti validate-env! "validates that the current environment is valid for the plugin " (fn [k env conf] (:codec conf)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;; Private Defaults and specific implementations for multi methods

(defmethod create-plugin :parquet [k file env conf]
  {:parquet (io-util/open-parquet-file! k file env conf)})


(defmethod create-plugin :default [_ file _ {:keys [codec out-buffer-size use-buffer] :or {codec :gzip out-buffer-size 32000 use-buffer false} :as conf}]
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
    (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))


(defmethod create-extension :default [codec]
  (cond
    (= codec :gzip) ".gz"
    (= codec :native-gzip) ".gz"
    (= codec :snappy) ".snz"
    (= codec :bzip2) ".bz2"
    (= codec :none) ".none"
    (= codec :parquet) ".parquet"
    :else
    (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))


(defmethod close-plugin :parquet [{:keys [parquet]}]
  (when parquet
    (io-util/close-parquet-file! parquet)))

(defmethod close-plugin :default [{:keys [out]}]
  (when out
    (doto ^OutputStream out .flush .close)))


(defmethod update-env! :parquet [k env _ & {:keys [parquet-codec message-type]}]
  (let [entry {:parquet-codec parquet-codec :message-type message-type}]
    (io-util/validate-parquet-conf! entry)
    (dosync
      (alter env assoc k entry))))

(defmethod update-env! :default [& _])


(defmethod validate-env! :parquet [k env conf]
  (io-util/validate-parquet-conf! (get @env k)))

(defmethod validate-env! :default [& _] true)