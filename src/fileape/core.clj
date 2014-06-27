(ns fileape.core
  (require
    [clojure.java.io :refer [make-parents]]
    [fileape.native-gzip :refer [create-native-gzip]]
    [fileape.bzip2 :refer [create-bzip2]]
    [fun-utils.core :refer [star-channel apply-get-create fixdelay stop-fixdelay]]
    [clojure.core.async :refer [go thread <! >! <!! >!! chan sliding-buffer]]
    [clojure.string :as clj-str]
    [clojure.tools.logging :refer [info error]])

  (import [java.util.concurrent.atomic AtomicReference AtomicInteger AtomicLong]
          [java.util.concurrent ThreadLocalRandom]
          [org.xerial.snappy SnappyOutputStream]
          [java.util.zip GZIPOutputStream]
          [java.io File BufferedOutputStream DataOutputStream FileOutputStream OutputStream IOException]))

(defn add-file-name-index [n i]
  (let [s1 (interpose "." (-> n (clj-str/split #"\.")))]
    (clj-str/join "" (flatten [(drop-last s1)
                               i
                               "."
                               (System/nanoTime)
                               "."
                               (last s1)]))))

(defn- create-future-file-name
  "Create the filename that would be written once the file has been rolled"
  [f i]
  (let [^File file (clojure.java.io/file f)]
    (str
      (.getParent file) "/" (add-file-name-index
                              (clj-str/join "" (interpose "_" (-> (.getName file) (clj-str/split #"_") drop-last))) i))
    file))

(defn th-rand-int
  "Returns a random number using the ThreadLocalRandom class from java 1.7"
  [^Long n]
  (-> (ThreadLocalRandom/current) (.nextLong n)))

(defn codec-extension [codec]
  (cond
    (= codec :gzip) ".gz"
    (= codec :native-gzip) ".gz"
    (= codec :snappy) ".snz"
    (= codec :bzip2) ".bz2"
    (= codec :none) ".none"
    :else
    (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))

(defn ^OutputStream get-output [^File file {:keys [codec] :or {codec :gzip}}]
  "Takes a file path and creates, creates an output stream using the correct codec
   and then returns it. possible values for the codec are :gzip :snappy :none"
  (cond
    (= codec :gzip)
    (let [zipout (DataOutputStream. (GZIPOutputStream. (BufferedOutputStream. (FileOutputStream. file) (int (* 10 1048576)))))]
      {:out zipout})
    (= codec :native-gzip)
    {:out (create-native-gzip file)}
    (= codec :bzip2)
    {:out (create-bzip2 file)}
    (= codec :snappy)
    {:out (DataOutputStream. (SnappyOutputStream. (BufferedOutputStream. (FileOutputStream. file) (int (* 10 1048576)))))}
    (= codec :none)
    {:out (DataOutputStream. (BufferedOutputStream. (FileOutputStream. file)))}
    :else
    (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))

(defn ^File create-file [base-dir codec file-key]
  "Create and return a File object with the name based on the file key codec and base dir"
  (File. (clojure.java.io/file base-dir)
         (str file-key
              (codec-extension codec)
              "_" (System/nanoTime))))

(defn- create-file-data!
  "Create a file and return a map with keys file codec file-key out,
   out contains the output stream and will always be a DataOutputStream
  Keys returned are: file, codec, file-key
  "
  [{:keys [codec base-dir] :as conf} file-key]
  (io!
    (let [^File file (create-file base-dir codec file-key)]
      (make-parents file)
      (.createNewFile file)
      (info "create new file " (.getAbsolutePath file))
      (if (not (.exists file)) (throw (IOException. (str "Failed to create " file))))

      (assoc
        (get-output file conf)
        :file  file
        :codec codec
        :file-key file-key
        :record-counter (AtomicLong. 0)
        :updated        (AtomicReference. (System/currentTimeMillis))))))

(defn- get-file-data!
  "Should always be called synchronized on file-key"
  [{:keys [file-map-ref] :as conf} file-key]
  (if-let [file-data (get @file-map-ref file-key)]
    file-data
    (let [file-data (create-file-data! conf file-key)]
      (dosync
        (alter file-map-ref assoc file-key file-data))
      file-data)))

(defn- write-to-file-data
  "Calls writer-f with the file-data"
  [file-data error-ch writer-f]
  (try
    (writer-f file-data)
    (catch Exception e (do
                         (.printStackTrace e)
                         (error e e)
                         (>!! error-ch e))))
  (.incrementAndGet ^AtomicLong (:record-counter file-data))
  (.set ^AtomicReference (:updated file-data) (System/currentTimeMillis)))

(defn- create-parallel-key
  "Create a key name that is based on file-key + rand[0 - parallel-files]"
  [{:keys [parallel-files]} file-key]
  (str file-key "." (th-rand-int parallel-files)))

(defn write
  "Writes the data in a thread safe manner to the file output stream based on the file-key
   If no output stream exists one is created"
  [{:keys [star error-ch] :as conn} file-key writer-f]
  (let [k (create-parallel-key conn file-key)]
    ((:send star) k
     ;this function will run in a channel in sync with other instances of the same topic
     #(write-to-file-data (get-file-data! conn k) error-ch %)
     writer-f
     )))

(defn close-and-roll
  "Close the output stream and rename the file by removing the last _[number] suffix"
  [{:keys [^File file ^OutputStream out] :as file-data} i]
  (io!
    (try
      (do
        (info "close and roll " file)
        (doto out .flush .close))
      (catch Exception e (prn e e)))
    (let [^File file2 (create-future-file-name file i)]
      (do
        (info "Rename " (.getName file) " to " (.getName file2))
        (.renameTo file file2)
        file2
        ))))

(defn roll-and-notify
  "Calls close-and-roll, then removes from file-map, and then notifies the roll-ch channel"
  [file-map-ref roll-ch file-data]
  (dosync
    (alter file-map-ref dissoc (:file-key file-data)))
  (prn ">>>>>>> close-and-roll " file-data)
  (let [file (close-and-roll file-data 0)]
    (>!! roll-ch (assoc file-data :file file))))

(defn close
  "Close each open file, and notify a roll event to the roll-ch channel
   any errors are reported to the error-ch channel"
  [{:keys [star file-map-ref error-ch roll-ch fix-delay-ch]}]
  (info "close !!!!!!!!!! " @file-map-ref)

  ;wait if the file map is empty
  ;some files might still be in the event loops
  (if (empty? @file-map-ref)
    (Thread/sleep 1000))

  (info "close !!!!  " @file-map-ref)
  (while (not-empty @file-map-ref)
    (doseq [[k file-data] @file-map-ref]
      (try
        (do
          ;we send close on the channel star, to coordinate close and writes
          (stop-fixdelay fix-delay-ch)
          ((:send star) true                                  ;;wait for response
           (:file-key file-data)
           [:remove #(roll-and-notify file-map-ref roll-ch %)] file-data))
        (catch Exception e (do (prn e e) (>!! error-ch e))))))

  ((:close star)))

(defn check-file-data-roll
  "Checks and if the file should be rolled its rolled"
  [{:keys [star file-map-ref roll-ch rollover-size rollover-timeout]}
   {:keys [file-key ^File file updated out] :as file-data}]
  (try
    (do
      ;(.flush out)
      ;(info "Check rollover-size[ " rollover-size "] < " (.getName file) " length " (.length file) " ts " (.get ^AtomicReference updated))
      (if (or (>= (.length file) rollover-size)
              (>= (- (System/currentTimeMillis) (.get ^AtomicReference updated)) rollover-timeout))
        ((:send star)
         file-key
         #(roll-and-notify file-map-ref roll-ch %) file-data)))
    (catch Exception e (error e e))))

(defn check-roll [{:keys [star file-map-ref] :as conn}]
  (try
    (doseq [[k file-data] @file-map-ref]
      (check-file-data-roll conn file-data))
    (catch Exception e (prn e e))))

(defn ape
  "Entrypoint to the api, creates the resources for writing"
  [{:keys [codec base-dir rollover-size rollover-timeout check-freq roll-callbacks
           parallel-files] :or {check-freq 10000 rollover-size 134217728 rollover-timeout 60000 parallel-files 3}}]
  (let [error-ch (chan (sliding-buffer 10))
        roll-ch (chan (sliding-buffer 100))
        file-map-ref (ref {})
        star (star-channel :wait-response false :buff 1000)
        conn
        {:codec            codec
         :star             star
         :base-dir         base-dir
         :file-map-ref     file-map-ref
         :rollover-size    rollover-size
         :rollover-timeout rollover-timeout
         :check-freq       check-freq
         :error-ch         error-ch
         :parallel-files   parallel-files
         :roll-ch          roll-ch}]

    (info "start file check " check-freq " rollover-sise " rollover-size " rollover-timeout " rollover-timeout)
    ;if any rollbacks
    (if roll-callbacks
      (go
        (loop []
          (if-let [v (<! roll-ch)]
            (do
              (try
                (doseq [f roll-callbacks]
                  (f v))

                (catch Exception e (prn e e)))
              (recur))))))

    (assoc conn :fix-delay-ch (fixdelay (:check-freq conn)
                                        (check-roll conn)))))
