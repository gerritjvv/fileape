(ns fileape.bzip2
  (:import [java.io FileOutputStream DataOutputStream]
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorOutputStream)))


(defn create-bzip2 [file]
  (DataOutputStream.
    (BZip2CompressorOutputStream.
      (FileOutputStream. (clojure.java.io/file file)))))
