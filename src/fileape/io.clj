(ns
  ^{:author "gerritjvv"
    :doc    "Basic api for doing async file IO using refs and agents"}
  fileape.io
  (:require
    [clojure.java.io :refer [make-parents] :as io]
    [clj-tuple :refer [tuple]]
    [fun-utils.threads :as threads]
    [fun-utils.agent :as fagent]
    [clojure.core.async :as async]
    [clojure.string :as clj-str]
    [fileape.native-gzip :refer [create-native-gzip]]
    [fileape.bzip2 :refer [create-bzip2]]
    [clojure.tools.logging :refer [info error debug]])
  (:import (java.util.concurrent.atomic AtomicReference AtomicLong AtomicBoolean)
           (java.io File IOException FileOutputStream BufferedOutputStream OutputStream DataOutputStream)
           (java.util.zip GZIPOutputStream)
           (org.xerial.snappy SnappyOutputStream)
           (java.util.concurrent CountDownLatch)))


(defrecord CTX [root-agent roll-ch conf shutdown-flag])

(defonce default-conf {:codec :gzip :threads 2})

(defn create-ctx [conf roll-ch]
  (->CTX (fagent/agent {}) roll-ch (merge default-conf conf) (AtomicBoolean. false)))


(defn open? [ctx]
  (not (.get ^AtomicBoolean (:shutdown-flag ctx))))


(defn- codec-extension [codec]
  (cond
    (= codec :gzip) ".gz"
    (= codec :native-gzip) ".gz"
    (= codec :snappy) ".snz"
    (= codec :bzip2) ".bz2"
    (= codec :none) ".none"
    :else
    (throw (RuntimeException. (str "The codec " codec " is not supported yet")))))

(defn- get-output
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


(defn create-future-file-name
  "Create the filename that would be written once the file has been rolled"
  [f]
  (let [^File file (io/file f)
        sp (-> (.getName file) (clj-str/split #"_"))
        n (clj-str/join "" (interpose "_" (drop-last sp)))  ;get name without
        sp2 (clj-str/split n #"\.")
        ]
    (str
      (.getParent file) "/" (clj-str/join "." (conj (vec (drop-last sp2)) (last sp) (last sp2))))))

(defn- select-base-dir [base-dir]
  (if (coll? base-dir)
    (rand-nth base-dir)
    base-dir))

(defn- close-and-roll
  "Close the output stream and rename the file by removing the last _[number] suffix"
  [{:keys [^File file ^OutputStream out ^File future-file-name]}]
  (when out
    (io!
      (doto out .flush .close)
      (let [^File file2 (if-not (.exists future-file-name)
                          future-file-name
                          (let [new-name (io/file (create-future-file-name file))]
                            (info "choosing new future file name " new-name " from " future-file-name)
                            new-name))]
        (info "close and roll " file " to " file2)
        (.renameTo file file2)
        file2))))

(defn- roll-and-notify
  "Calls close-and-roll, then removes from file-map, and then notifies the roll-ch channel"
  [roll-ch file-data]
  (when file-data
    (let [file (close-and-roll file-data)]
      (async/>!! roll-ch (assoc file-data :file file))))
  file-data)


(defn- ^File create-file
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
      :future-file-name (io/file (create-future-file-name file))
      :record-counter (AtomicLong. 0)
      :updated (AtomicReference. (System/currentTimeMillis)))))

(defn- create-agent [conf k]
  ;create an agent with a default mailbox-len of 10
  (fagent/agent (create-file-data! conf k) :mailbox-len 10))


(defn- create-if-not
  "Returns [(agent (delay file-data)) map]"
  [conf k m]
  (if-let [o (get m k)] (tuple o m) (let [o (create-agent conf k)] (tuple o (assoc m k o)))))

(defn- write-to-agent!
  "Calls the writer-f via the agent sending it the agents dereferenced result"
  [ctx writer-f [agnt m :as tpl]]
  ;@TODO find a better mechanism to not block forever
  (fagent/send agnt (fn [file-desc] (try (writer-f file-desc) (catch Exception e (error e e))) file-desc))
  tpl)

(defn- do-roll!
  "Helper function that calls roll-and-notify"
  [check-f ctx file-desc]
  (roll-and-notify (:roll-ch ctx) file-desc)
  nil)


(defn- reducer-roll-if
  "Loops through each item in the map m and if check-f returns true calls do-roll and removed the key from m:map
   The final m:map is returned"
  [ctx check-f m & {:keys [close-and-wait] :or {close-and-wait false}}]
  (reduce-kv (fn [m k agnt]
               ;agnt is an agent and its value is (delay file-desc)
               (debug "check file for roll " (:file @agnt) " should roll " (check-f @agnt) " close-and-wait " close-and-wait)
                (if (check-f @agnt)
                  (if (fagent/send agnt (partial do-roll! check-f ctx))
                    (do
                      (fagent/close-agent agnt :wait close-and-wait)
                      (dissoc m k))
                    m)
                  m))
             m m))

(defn check-roll!
  "helper function that calls reducer-roll-if in a transaction and alters the ref (:state ctx)"
  [ctx check-f & {:keys [close-and-wait] :or {close-and-wait false}}]
  (fagent/send (:root-agent ctx)
               #(reducer-roll-if ctx check-f % :close-and-wait close-and-wait)))


(defn async-write!
  "Creates a file based on the key k, the file descriptor is cached so that its only created once
   The descriptor is passed to the writer-f, and the final state is maintained in the (:state ctx)"
  [ctx k writer-f]
  (if (open? ctx)
    (fagent/send (:root-agent ctx)
                 (comp #(nth % 1)
                       (comp (partial write-to-agent! ctx writer-f)
                             (partial create-if-not (:conf ctx) k))))
    (throw (RuntimeException. "The writer context has already been closed"))))


(defn shutdown! [ctx]
  (.set ^AtomicBoolean (:shutdown-flag ctx) true)
  (check-roll! ctx (fn [& _] true) :close-and-wait true)
  (fagent/close-agent (:root-agent ctx) :wait true))