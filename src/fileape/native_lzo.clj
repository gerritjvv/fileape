(ns fileape.native-lzo
  (:refer-clojure :exclude [write])
  (:require [clojure.core.async :refer [chan <!! >!! dropping-buffer]])
  (:import  [java.io OutputStream FileOutputStream BufferedOutputStream DataOutputStream]
            [org.apache.hadoop.io.compress Compressor]
            [com.hadoop.compression.lzo LzopCodec]
            [org.apache.hadoop.conf Configurable Configuration]))

(defonce compressor-cache-size 100)


(defonce conf (Configuration.))

(defonce ^LzopCodec lzo-codec (do (let [codec (LzopCodec.)]
                                     (if (instance? Configurable codec)
                                       (.setConf codec conf))
                                     codec)))

;use a channel as a cache, the dropping buffer is not required but 
;helps for sanity that if we put back an item the put back will never block
(defonce compressor-cache (delay
                            (let [cache (chan (dropping-buffer (inc compressor-cache-size)))]
                              (dotimes [i compressor-cache-size]
                                (>!! cache (.createCompressor lzo-codec)))
                              cache)))

(defn create-native-lzo [file]
  
  (if (nil? (.createCompressor lzo-codec))
    (throw (RuntimeException. "No native gzip library found")))
  
  (let [^Compressor compressor (<!! @compressor-cache)
        ^OutputStream fout (BufferedOutputStream. (FileOutputStream. (clojure.java.io/file file)) (* 10 1048576))
        ^OutputStream out  (.createOutputStream lzo-codec fout compressor)]
  (proxy [DataOutputStream]
    [out]
    (close []
      (.finish compressor)
      (.close out)
      (.close fout)
      (.reset compressor)
      (>!! @compressor-cache compressor)))))
