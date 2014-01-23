(ns fileape.core
  (require 
         [clojure.java.io :refer [make-parents]]
         [fun-utils.core :refer [star-channel apply-get-create fixdelay]]
         [clojure.core.async :refer [go <! >! <!! >!! chan sliding-buffer ]]
         [clojure.string :as clj-str])
  (import [java.util.concurrent.atomic AtomicReference]
          [java.util.zip GZIPOutputStream]
          [java.io File BufferedOutputStream DataOutputStream FileOutputStream OutputStream]))
		    
  
  (defn codec-extension [codec]
    (cond 
      (= codec :gzip)   ".gz"
      (= codec :lzo)    ".lzo"
      (= codec :snappy) ".snz"
      (= codec :bzip2)  ".bz2"
      (= codec :none) ".none"
      :else 
      (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))
  
  (defn ^OutputStream get-output [^File file {:keys [codec] :or {codec :gzip}}]
    "Takes a file path and creates, creates an output stream using the correct codec
     and then returns it. possible values for the codec are :gzip :lzo :snappy :none"
    (cond 
      (= codec :gzip)
      (let [zipout (DataOutputStream. (GZIPOutputStream. (BufferedOutputStream. (FileOutputStream. file) (int (* 10 1048576))) ))]
        {:out zipout})
      (= codec :none)
        {:out (DataOutputStream. (BufferedOutputStream. (FileOutputStream. file)))}
      :else
      (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))
 
  (defn ^File create-file [base-dir codec file-key]
    "Create and return a File object with the name based on the file key codec and base dir"
       (File. base-dir (str file-key 
                            (codec-extension codec)
                            "_" (System/nanoTime))))
      
 
  (defn create-file-data [{:keys [codec base-dir] :as conf} file-key]
    "Create a file and return a map with keys file codec file-key out,
     out contains the output stream and will always be a DataOutputStream"
    (let [file (create-file base-dir codec file-key)]
      (make-parents file)
      (.createNewFile file)
      (if (not (.exists file)) (throw (java.io.IOException. (str "Failed to create " file)))  )
      
      (merge {:file file :codec codec :file-key file-key :updated (AtomicReference. (System/currentTimeMillis))} 
             (get-output file conf))))
  
  (defn get-file-data [{:keys [file-map-ref] :as conf} file-key]
    (dosync
        (if-let [file-data (get @file-map-ref file-key)]
                 file-data
                 (get 
                   (alter file-map-ref (fn [m] 
                                         (if (not (get file-key m))
                                             (assoc m file-key (create-file-data conf file-key))
                                             m)))
                   file-key))))
                   
                   
   (defn write-to-file-data [file-data writer-f]
     "Extracts the output stream from file-data and calls writer-f with the former as its argument"
     (writer-f (:out file-data))
     (.set (:updated file-data) (System/currentTimeMillis)))
  
  (defn write [{:keys [star] :as conn} file-key writer-f]
    "Writes the data in a thread safe manner to the file output stream based on the file-key
     If no output stream exists one is created"
		  ((:send star) file-key
                          ;this function will run in a channel in sync with other instances of the same topic
		                      (fn [writer-f]
		                          (write-to-file-data (get-file-data conn file-key) writer-f))
                          writer-f
                          ))
  
  (defn add-file-name-index [n i]
    (if (= i 0)
      n
	    (let [s1 (interpose "." (-> n (clj-str/split #"\.")))]
	         (clj-str/join "" (flatten  [(drop-last s1) 
	                                         i
	                                         "."
	                                      (last s1)])))))
         
  (defn close-and-roll [{:keys [file ^OutputStream out] :as file-data} i]
    "Close the output stream and rename the file by removing the last _[number] suffix"
    (try
      (do 
        (.flush out)
        (.close out))
      (catch Exception e (.printStackTrace e)))
    
    (let [file2 (File. (.getParent file)
                       (add-file-name-index
                         (clj-str/join "" (interpose "_" (-> (.getName file) (clj-str/split #"_") drop-last)))
                         i))]
      (if (.exists file2)
        (close-and-roll file-data (inc i))
        (.renameTo file file2))))
  
   (defn roll-and-notify [file-map-ref roll-ch file-data]
     "Calls close-and-roll, then removes from file-map, and then notifies the roll-ch channel"
     (dosync alter file-map-ref (fn [m] (dissoc m (:file-key file-data))))
     (close-and-roll file-data 0)
     (>!! roll-ch file-data))
     
  (defn close [{:keys [star file-map-ref error-ch roll-ch]}]
    "Close each open file, and notify a roll event to the roll-ch channel
     any errors are reported to the error-ch channel"
    (doseq [[k file-data] @file-map-ref]
      (try 
        (do
          ;we send close on the channel star, to coordinate close and writes
           ((:send star) true ;;wait for response
                         (:file-key file-data)
                         #(roll-and-notify file-map-ref roll-ch %) file-data))
        
        (catch Exception e (>!! error-ch e)))))
  
  ;;{:keys [star file-map-ref roll-ch rollover-size rollover-timeout]} 
  ;;                            {:keys [file-key ^File file updated out] :as file-data}
  
   (defn check-file-data-roll [{:keys [star file-map-ref roll-ch rollover-size rollover-timeout]} 
                              {:keys [file-key ^File file updated out] :as file-data}]
    "Checks and if the file should be rolled its rolled"
	    (.flush out)
	    (if (or (>= (.length file) rollover-size)
	            (>= (- (System/currentTimeMillis) (.get ^AtomicReference updated)) rollover-timeout))
	      ((:send star) file-key
	                    #(roll-and-notify file-map-ref roll-ch %) file-data)))
	    
  (defn check-roll [{:keys [star file-map-ref] :as conn}]
    (doseq [[k file-data] @file-map-ref]
      ((:send star) k
                    #(check-file-data-roll conn %) file-data)))
  
  (defn start-services [conn]
     (fixdelay (:check-freq conn)
                (check-roll conn)))
  
  (defn ape [{:keys [codec base-dir rollover-size rollover-timeout check-freq] :or {check-freq 10000 rollover-size 134217728 rollover-timeout 60000}}]
    "Entrypoint to the api, creates the resources for writing"
     (let [error-ch (chan (sliding-buffer 10))
           roll-ch (chan (sliding-buffer 100))
           file-map-ref (ref {})
           star (star-channel :wait-response false :buff 1000)
           ;TODO start service like rollover checks etc
           ;TODO provide shutdown function
           conn
	               {:codec codec 
					        :star star
					        :base-dir base-dir
					        :file-map-ref file-map-ref
					        :rollover-size rollover-size
					        :rollover-timeout rollover-timeout
					        :check-freq check-freq
					        :error-ch error-ch
					        :roll-ch roll-ch}]
	       (start-services conn)
	       conn))