(ns fileape.native-gzip
  (:refer-clojure :exclude [write])
  (:require [clojure.core.async :refer [chan <!! >!! dropping-buffer]])
  (:import  [java.io OutputStream FileOutputStream BufferedOutputStream DataOutputStream]
            [org.apache.hadoop.io.compress Compressor GzipCodec]
            [org.apache.hadoop.conf Configurable Configuration]))

(defonce compressor-cache-size 100)


(defonce conf (Configuration.))

(defonce ^GzipCodec gzip-codec (do (let [codec (GzipCodec.)]
                                     (if (instance? Configurable codec)
                                       (.setConf codec conf))
                                     codec)))

;use a channel as a cache, the dropping buffer is not required but 
;helps for sanity that if we put back an item the put back will never block
(defonce compressor-cache (delay
                            (let [cache (chan (dropping-buffer (inc compressor-cache-size)))]
                              (dotimes [i compressor-cache-size]
                                (>!! cache (.createCompressor gzip-codec)))
                              cache)))

(defn create-native-gzip [file]
  
  (if (nil? (.createCompressor gzip-codec))
    (throw (RuntimeException. "No native gzip library found")))
  
  (let [^Compressor compressor (<!! @compressor-cache)
        ^OutputStream fout (BufferedOutputStream. (FileOutputStream. (clojure.java.io/file file)) (* 10 1048576))
        ^OutputStream out  (.createOutputStream gzip-codec fout compressor)]
  (proxy [DataOutputStream]
    [out]
    (close []
      (.finish compressor)
      (.close out)
      (.close fout)
      (.reset compressor)
      (>!! @compressor-cache compressor)))))
