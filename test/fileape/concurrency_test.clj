(ns fileape.concurrency-test
  (:require [clojure.test :refer :all]
            [fileape.core :refer :all]
            [midje.sweet :refer :all]
            [clojure.tools.logging :refer [info]])
  (:import [java.io File DataOutputStream FileInputStream DataInputStream]
          [java.util.zip GZIPInputStream]
          [java.io File]))

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
             (create-future-file-name "blabla_abc.gz_000") => "/blabla_abc.000.gz")

         (fact "Write and read Gzip File"

               (let [base-dir (File. "target/tests/concurrent-write-read-gzip1")
                     _ (do
                         (doall (map #(clojure.java.io/delete-file % :silently) (file-seq base-dir))))
                     ape2 (ape {:codec :gzip :base-dir base-dir :check-freq 1000 :rollover-timeout 1000})
                     key-stream1 (take 1 (cycle (map str (range 0 5))))
                     key-stream2 (take 1 (cycle (map str (range 10 20))))

                     thread-count 10
                     run-test (fn [key-stream]
                                (doall
                                  (repeatedly thread-count
                                              #(future
                                                (doseq [i (range 10000)]
                                                  (let [ bts (.getBytes (str i "\n")) ]
                                                    (write ape2 (str "abc-" (rand-nth key-stream))
                                                           (fn [{:keys [^DataOutputStream out]}] (.writeInt out (int i))))))))))]

                 (doall (map deref (run-test key-stream1)))
                 (doall (map deref (run-test key-stream2)))
                 (doall (map deref (run-test key-stream1)))

                 (Thread/sleep 2000)

                 ;find the rolled file and read back

                 (close ape2)

                 (let [files (filter (fn [^File file] (re-find #"abc-" (.getName file))) (.listFiles base-dir))]
                   (> (count files) 0) => true
                   (count (flatten (read-files files))) => 300000)

                 (shutdown-agents))))
