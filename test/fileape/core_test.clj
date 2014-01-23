(ns fileape.core-test
  (:require [clojure.test :refer :all]
            [fileape.core :refer :all]
            [midje.sweet :refer :all])
  (import [java.io File DataOutputStream FileInputStream DataInputStream]
          [java.util.concurrent.atomic AtomicInteger]
          [java.util.zip GZIPInputStream]
          [java.io OutputStream]))

(facts "Test file writing"
       
      
       (fact "test write file"
             
             (let [write-count (AtomicInteger.)
                   ape (ape {:codec :gzip})]
               (write ape "abc-123" (fn [^DataOutputStream o] (.getAndIncrement write-count)
                                                 (.writeInt o (int 1))))
               
               (Thread/sleep 500)
               (.get write-count) => 1
               
               ))
       
         (fact "Write and read Gzip File"
               (let [base-dir (File. "target/tests/write-read-gzip")
                     ape2 (ape {:codec :gzip :base-dir base-dir})]
                 
                 (doseq [i (range 100)]
                   (let [ bts (.getBytes (str i "\n")) ]
                   (write ape2 "abc-123" (fn [^DataOutputStream o] (.writeInt o (int i))))))
                   
                 (close ape2)
                 
                 ;find the rolled file and read back
                 (let [file (first (filter (fn [^File file] (re-find #"abc-123" (.getName file))) (.listFiles base-dir)))]
                   (nil? file) => false
                   (sort 
                     (take 100 (let [o (-> file FileInputStream. GZIPInputStream. DataInputStream.)]
                                     (repeatedly #(.readInt o))))) => (range 100)
                                      
                 
               )))
         (fact "Write speed test"
	               (let [base-dir (File. "target/tests/write-speed-test")
	                     ape2 (ape {:codec :gzip :base-dir base-dir})
                       bts-1mb (.getBytes (apply str (take 1048576 (repeatedly (fn [] "a")))))
                       bts-len (count bts-1mb)
                       start (System/currentTimeMillis)]
	                 
                   ;write out 1gig of raw data
                   
	                 (doseq [^bytes mb-bts (take 1000 (repeatedly (fn [] bts-1mb)))]
	                   (write ape2 "abc-123" (fn [^DataOutputStream o] (.write o mb-bts 0 (int bts-len)  ))))
	                 
                   (let [end (System/currentTimeMillis)]
                     (close ape2)
	                   (prn "Time taken to write 1000 mb " (- end start) " ms"))
			                 
	                                      
	                 
	               ))
            
	          (fact "Test Services rollover timeout"
		               (let [base-dir (File. "target/tests/write-test-rollover-timeout")
		                     ape2 (ape {:codec :gzip :base-dir base-dir :check-freq 1000 :rollover-timeout 200})
	                       start (System/currentTimeMillis)]
		                 
	                   (write ape2 "abc-123" (fn [^DataOutputStream o] (.writeInt o (int 1))))
	                   (Thread/sleep 1300)
	                   ;we expect a file here
	                   (let [file (first (filter (fn [^File file] (re-find #"abc-123" (.getName file))) (.listFiles base-dir)))]
	                     (nil? file) => false
	                     (.exists file) => true)
		                                      
		                 (close ape2)
		               ))
          
           (fact "Test Services rollover size"
		               (let [base-dir (File. "target/tests/write-test-rollover-size")
		                     ape2 (ape {:codec :gzip :base-dir base-dir :check-freq 5000 :rollover-size 100})
                         bts-1mb (.getBytes (apply str (take 1048576 (repeatedly (fn [] "a")))))
                         bts-len (count bts-1mb)
                       
	                       start (System/currentTimeMillis)]
		                 
                   
	                    (doseq [^bytes mb-bts (take 1000 (repeatedly (fn [] bts-1mb)))]
                        (write ape2 "abc-123" (fn [^DataOutputStream o] (.write o mb-bts 0 (int bts-len)  ))))
	                
	                  (while true (Thread/sleep 2000))
                    
	                   ;we expect a file here
	                   (let [files (filter (fn [^File file] (re-find #"abc-123" (.getName file))) (.listFiles base-dir))]
	                     (prn "Files " (count files)))
		                                      
		                 (close ape2)
		               ))
	          )