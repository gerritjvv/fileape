(ns fileape.bzip2
  (:refer-clojure :exclude [write])
  (:require [clojure.core.async :refer [chan <!! >!! dropping-buffer]])
  (:import  [java.io OutputStream FileOutputStream BufferedOutputStream DataOutputStream]
            [org.apache.hadoop.io.compress Compressor BZip2Codec]
            [org.apache.hadoop.conf Configurable Configuration]))

(defonce compressor-cache-size 100)

(defonce conf (Configuration.))

(defonce ^BZip2Codec bzip2-codec (do (let [codec (BZip2Codec.)]
                                     (if (instance? Configurable codec)
                                       (.setConf ^Configurable codec conf))
                                     codec)))

;use a channel as a cache, the dropping buffer is not required but 
;helps for sanity that if we put back an item the put back will never block
(defonce compressor-cache (delay
                            (let [cache (chan (dropping-buffer (inc compressor-cache-size)))]
                              (dotimes [i compressor-cache-size]
                                (>!! cache (.createCompressor bzip2-codec)))
                              cache)))


(defn create-bzip2 [file]
  (let [^Compressor compressor (<!! @compressor-cache)
        ^OutputStream fout (BufferedOutputStream. (FileOutputStream. (clojure.java.io/file file)) (* 10 1048576))
        ^OutputStream out  (.createOutputStream bzip2-codec fout compressor)]
  (proxy [DataOutputStream]
    [out]
    (close []
      (.finish compressor)
      (.close out)
      (.close fout)
      (.reset compressor)
      (>!! @compressor-cache compressor)))))
