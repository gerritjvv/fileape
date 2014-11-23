;;see https://github.com/gerritjvv/fileape for usage
(ns fileape.core
  (:import (fileape.io ActorPool$Command)
           (clojure.lang IFn))
  (require
    [clojure.java.io :refer [make-parents] :as io]
    [fileape.native-gzip :refer [create-native-gzip]]
    [fileape.bzip2 :refer [create-bzip2]]
    [fun-utils.core :refer [apply-get-create fixdelay-thread stop-fixdelay go-seq]]
    [clojure.core.async :refer [go thread <! >! <!! >!! chan sliding-buffer]]
    [clojure.string :as clj-str]
    [clojure.tools.logging :refer [info error]])

  (import [fileape.io Actor ActorPool]
          [java.util.concurrent.atomic AtomicReference AtomicInteger AtomicLong]
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

(defn create-future-file-name
  "Create the filename that would be written once the file has been rolled"
  [f i]
  (let [^File file (io/file f)
        sp (-> (.getName file) (clj-str/split #"_"))
        n (clj-str/join "" (interpose "_" (drop-last sp)))  ;get name without
        sp2 (clj-str/split n #"\.")
        ]
    (str
      (.getParent file) "/" (clj-str/join "." (conj (vec (drop-last sp2)) (last sp) (last sp2))))))

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

(defn ^OutputStream get-output
  "Takes a file path and creates, creates an output stream using the correct codec
   and then returns it. possible values for the codec are :gzip :snappy :none"
  [^File file {:keys [codec] :or {codec :gzip}}]
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

(defn- select-base-dir [base-dir]
  (if (coll? base-dir)
    (rand-nth base-dir)
    base-dir))

(defn ^File create-file
  "Create and return a File object with the name based on the file key codec and base dir"
  [base-dir codec file-key]
  (let [file (File. (io/file (select-base-dir base-dir))
                    (str file-key
                         (codec-extension codec)
                         "_" (System/nanoTime)))]
    ;check that the file does not exist
    (if (.exists file)
      (do
        (Thread/sleep 200)
        (create-file base-dir codec file-key))
      file)))

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
      (if-not (.exists file) (throw (IOException. (str "Failed to create " file))))

      (assoc
          (get-output file conf)
        :file file
        :codec codec
        :file-key file-key
        :future-file-name (io/file (create-future-file-name file 0))
        :record-counter (AtomicLong. 0)
        :updated (AtomicReference. (System/currentTimeMillis))))))


(defn- write-to-file-data
  "Calls writer-f with the file-data"
  [file-data error-ch writer-f]
  (try
    (writer-f file-data)
    (catch Exception e (do
                         (error file-data)
                         (.printStackTrace e)
                         (error e e)
                         (>!! error-ch e))))
  (.incrementAndGet ^AtomicLong (:record-counter file-data))
  (.set ^AtomicReference (:updated file-data) (System/currentTimeMillis))
  file-data)

(defn- create-parallel-key
  "Create a key name that is based on file-key + rand[0 - parallel-files]"
  [{:keys [parallel-files]} file-key]
  (str file-key "." (th-rand-int parallel-files)))

(defn write
  "Writes the data in a thread safe manner to the file output stream based on the file-key
   If no output stream exists one is created"
  [{:keys [^ActorPool actor-pool error-ch ^IFn file-data-create] :as conn} file-key writer-f]
  (let [^String k (create-parallel-key conn file-key)
        ^IFn f (fn [file-data]
                 (try
                   (write-to-file-data file-data error-ch writer-f)
                   (catch Exception e (error (str "Error file-data " file-data)))
                   (finally file-data)))]
    (.send                                                  ;IFn createStateFn, String key, IFn fn
     actor-pool
     file-data-create
     k
     f)))

(defn close-and-roll
  "Close the output stream and rename the file by removing the last _[number] suffix"
  [{:keys [^File file ^OutputStream out ^File future-file-name] :as file-data} i]
  (when out
    (io!
      (doto out .flush .close)
      (let [^File file2 (if-not (.exists future-file-name)
                          future-file-name
                          (io/file (create-future-file-name file 1)))]
        (info "close and roll " file " to " file2)
        (.renameTo file file2)
        file2))))

(defn roll-and-notify
  "Calls close-and-roll, then removes from file-map, and then notifies the roll-ch channel"
  [roll-ch file-data]
  (when file-data
    (let [file (close-and-roll file-data 0)]
      (>!! roll-ch (assoc file-data :file file))
      file-data)))

(defn close
  "Close each open file, and notify a roll event to the roll-ch channel
   any errors are reported to the error-ch channel"
  [{:keys [^ActorPool actor-pool  error-ch roll-ch fix-delay-ch file-data-create]}]

  (stop-fixdelay fix-delay-ch)
  (.sendCommandAll
    actor-pool
    ActorPool$Command/DELETE
    file-data-create
    #(roll-and-notify roll-ch %))
  (.shutdown actor-pool 5000))

(defn check-file-data-roll
  "Checks and if the file should be rolled its rolled"
  [{:keys [^ActorPool actor-pool roll-ch rollover-size rollover-timeout rollover-abs-timeout file-data-create]}
   {:keys [file-key ^File file updated out] :as file-data}]
  (try
    (let [tm-diff (- (System/currentTimeMillis) (.get ^AtomicReference updated))]
      (when (or (>= (.length file) rollover-size)
                (>= tm-diff rollover-timeout)
                (>= tm-diff rollover-abs-timeout))
        ;we need to remove the file key waiting for the remove to complete

        (.sendCommand
         actor-pool
         ActorPool$Command/DELETE
         file-data-create
         (:file-key file-data)
         #(roll-and-notify roll-ch %))))
    (catch Exception e (error e e))
    (finally file-data)))

(defn check-roll [{:keys [^ActorPool actor-pool file-data-create] :as conn}]
  (try
    (.sendCommandAll
      actor-pool
      ActorPool$Command/CHECK_ROLL
      file-data-create
      (partial check-file-data-roll conn))
    (catch Exception e (error e e))))

(defn ape
  "Entrypoint to the api, creates the resources for writing"
  [{:keys [codec base-dir rollover-size rollover-timeout rollover-abs-timeout check-freq roll-callbacks
           parallel-files] :or {check-freq 10000 rollover-size 134217728 rollover-timeout 60000 parallel-files 3 rollover-abs-timeout Long/MAX_VALUE}}]
  (let [error-ch (chan (sliding-buffer 10))
        roll-ch (chan (sliding-buffer 100))
        actor-pool (ActorPool/newInstance)
        conn
        {:codec            codec
         :actor-pool       actor-pool
         :base-dir         base-dir
         :rollover-size    rollover-size
         :rollover-timeout rollover-timeout
         :check-freq       check-freq
         :error-ch         error-ch
         :parallel-files   parallel-files
         :rollover-abs-timeout rollover-abs-timeout
         :roll-ch          roll-ch
         :file-data-create (fn [& args])}]

    (info "start file check " check-freq " rollover-sise " rollover-size " rollover-timeout " rollover-timeout)
    ;if any rollbacks
    (when (not-empty roll-callbacks)
      (go-seq
        (fn [v]
          (try
            (->> roll-callbacks (map #(% v)) doall)
            (catch Exception e (prn e e))))
        roll-ch))

    (assoc conn
           :file-data-create (fn [k]
                               (create-file-data! conn k))
           :fix-delay-ch (fixdelay-thread (:check-freq conn) (check-roll conn)))))
