(ns fileape.core-test
  (:require [clojure.test :refer :all]
            [fileape.core :refer :all]
            [midje.sweet :refer :all])
  (import [java.io File DataOutputStream FileInputStream DataInputStream]
          [java.util.concurrent.atomic AtomicInteger]
          [java.util.zip GZIPInputStream]
          [java.io File OutputStream]))

(defn read-file [file]
  (let [o (-> (clojure.java.io/file file) FileInputStream. GZIPInputStream. DataInputStream.)
        f (fn []
            (try (.readInt o) (catch Exception e nil)))]
    (take-while #(not (nil? %)) (repeatedly f))))


(defn read-files [files]
  (if-let [file (first files)]
           (lazy-seq (cons (read-file file) (read-files (rest files))))))

(facts "Test file writing"
       (fact "Test future file name rollover"
             (create-future-file-name "blabla_abc.gz_000" 0) => "/blabla_abc.000.gz")

       (fact "test write file"

             (let [write-count (AtomicInteger.)
                   ape (ape {:codec :gzip})]
               (write ape "abc-123" (fn [{:keys [^DataOutputStream out]}] (.getAndIncrement write-count)
                                      (.writeInt out (int 1))))

               (Thread/sleep 500)
               (.get write-count) => 1))

         (fact "Write and read Gzip File"
               (let [base-dir (File. "target/tests/write-read-gzip1")
                     ape2 (ape {:codec :gzip :base-dir base-dir})]

                 (doall (map #(clojure.java.io/delete-file % :silently) (file-seq base-dir)))

                 (doseq [i (range 100)]
                   (let [ bts (.getBytes (str i "\n")) ]
                   (write ape2 "abc-123" (fn [{:keys [^DataOutputStream out]}] (.writeInt out (int i))))))

                 (Thread/sleep 2000)
                 (close ape2)

                 (Thread/sleep 2000)
                 ;find the rolled file and read back

                 (let [files (filter (fn [^File file] (re-find #"abc-123" (.getName file))) (.listFiles base-dir))]
                   (> (count files) 0) => true
                   (sort
                     (take 100 (flatten (read-files files))))  => (range 100))))

         (fact "Write speed test"
               (let [base-dir (File. "target/tests/write-speed-test")
                     ape2 (ape {:codec :gzip :base-dir base-dir})
                     bts-1mb (.getBytes (apply str (take 1048576 (repeatedly (fn [] "a")))))
                     bts-len (count bts-1mb)
                     start (System/currentTimeMillis)]

                 (doall (map #(clojure.java.io/delete-file % :silently) (file-seq base-dir)))
                 ;write out 1gig of raw data

                 (doseq [^bytes mb-bts (take 1000 (repeatedly (fn [] bts-1mb)))]
                   (write ape2 "abc-123" (fn [{:keys [^DataOutputStream out]}] (.write out mb-bts 0 (int bts-len)  ))))

                 (let [end (System/currentTimeMillis)]
                   (close ape2)
                   (prn "Time taken to write 1000 mb " (- end start) " ms"))))

         (fact "Test Services rollover timeout"
               (let [base-dir (File. "target/tests/write-test-rollover-timeout")
                     ape2 (ape {:codec :gzip :base-dir base-dir :check-freq 1000 :rollover-timeout 200})
                     start (System/currentTimeMillis)]

                 (doall (map #(clojure.java.io/delete-file % :silently) (file-seq base-dir)))

                 (write ape2 "abc-123" (fn [{:keys [^DataOutputStream out]}] (.writeInt out (int 1))))
                 (Thread/sleep 2000)
                 ;we expect a file here
                 (let [file (first (filter (fn [^File file] (re-find #"abc-123" (.getName file))) (.listFiles base-dir)))]
                   (prn ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> file " file " list files " (map  #(.getName % ) (.listFiles base-dir)))
                   (nil? file) => false
                   (.exists file) => true)

                 (close ape2)))

         (fact "Test Services rollover size"
               (let [base-dir (File. "target/tests/write-test-rollover-size")
                     ape2 (ape {:codec :gzip :base-dir base-dir :check-freq 5000 :rollover-size 100})
                     bts-1mb (.getBytes (apply str (take 1048576 (repeatedly (fn [] "a")))))
                     bts-len (count bts-1mb)

                     start (System/currentTimeMillis)]

                 (doall (map #(clojure.java.io/delete-file % :silently) (file-seq base-dir)))

                 (doseq [^bytes mb-bts (take 1000 (repeatedly (fn [] bts-1mb)))]
                   (write ape2 "abc-123" (fn [{:keys [^DataOutputStream out]}] (.write out mb-bts 0 (int bts-len)  ))))

                 ;we expect a file here
                 (let [files (filter (fn [^File file] (re-find #"abc-123" (.getName file))) (.listFiles base-dir))]
                   (prn "Files " (count files)))

                 (close ape2))))
