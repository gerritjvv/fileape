(ns
  ^{:author "gerritjvv"
    :doc    "Basic api for doing async file IO using refs and agents"}
  fileape.io
  (:require
    [clojure.java.io :as io]
    [fun-utils.agent :as fagent]
    [clojure.core.async :as async]
    [clojure.string :as clj-str]
    [fileape.io-plugin :as io-plugin]
    [clojure.tools.logging :refer [info error debug]])
  (:import (java.util.concurrent.atomic AtomicReference AtomicLong AtomicBoolean)
           (java.io File IOException)
           (org.apache.commons.io FileUtils)
           (java.nio.file Files CopyOption StandardCopyOption)))


(defrecord CTX [root-agent roll-ch conf env shutdown-flag error-ch])

(defonce default-conf {:codec :gzip :threads 2})

(defn create-ctx
  "error-ch is called with [error function-sent-to-agent]"
  ([conf env roll-ch]
   (create-ctx conf env roll-ch (async/chan (async/sliding-buffer 1))))
  ([conf env roll-ch error-ch]
   {:pre [conf env roll-ch error-ch]}
   (->CTX (fagent/agent {} :error-handler (fn [e f]
                                            (error (str e " from " f) e)
                                            (async/>!! error-ch [e f])))
          roll-ch (merge default-conf conf) env (AtomicBoolean. false) error-ch)))


(defn update-ctx
  "Returns a new CTX with a new config applied, all other values are kept as is"
  [{:keys [root-agent roll-ch env shutdown-flag error-ch]} conf]
  (->CTX root-agent roll-ch conf env shutdown-flag error-ch))

(defn open? [ctx]
  (not (.get ^AtomicBoolean (:shutdown-flag ctx))))


(defn- codec-extension [codec]
  (io-plugin/create-extension codec))

(defn- get-output
  "Takes a file path and creates, creates an output stream using the correct codec
   and then returns it. possible values for the codec are :gzip :snappy :none :parquet
    note the :out for almost all are OutputStreams except for parquet which has its own format
    and use the :parquet key"
  [k ^File file env {:keys [codec out-buffer-size use-buffer] :as conf}]
  {:pre [k file codec]}
  (info "creating file for " k " codec " codec " with out-buffer-size " out-buffer-size " use-buffer " use-buffer)
  (io-plugin/create-plugin k file env conf))


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

(defn move-file [^File source-file ^File dest-file]
  (try
    ;;try our best to do an atomic rename to avoid partial move files being seen, but this is platform dependant
    (Files/move (.toPath source-file) (.toPath dest-file) (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE StandardCopyOption/COPY_ATTRIBUTES]))
    (catch Exception e
      (do
        (debug e (str "Error while moving " source-file " to " dest-file))
        (FileUtils/moveFile source-file dest-file))            ;note: rename is platform dependant and might not even be atomic, causing
                                                            ;other processes to see the file and then fail on file doens't exist, or see
                                                            ;only parts of the file. Using move here ensures to move has happened.
      )))

(defn- close-and-roll
  "Close the output stream and rename the file by removing the last _[number] suffix"
  [{:keys [file future-file-name] :as file-data}]
  (io!
    (io-plugin/close-plugin file-data)
    (when (.exists (io/file file))
      (let [^File file2 (if-not (.exists (io/file future-file-name))
                          future-file-name
                          (let [new-name (io/file (create-future-file-name file))]
                            (info "choosing new future file name " new-name " from " future-file-name)
                            new-name))]
        (info "close and roll " file " to " file2)

        (move-file file (io/file file2))
        file2))))

(defn- roll-and-notify
  "Calls close-and-roll, then removes from file-map, and then notifies the roll-ch channel
   The roll-ch is only sent keys [file codec file-key future-file-name ^AtomicLong record-count ^AtomicReference(long) updated]"
  [roll-ch file-data]
  (when file-data
    (let [file (close-and-roll file-data)]
      (async/>!! roll-ch {:file             file
                          :codec            (:codec file-data)
                          :file-key         (:file-key file-data)
                          :future-file-name (:future-file-name file-data)
                          :record-counter   (:record-counter file-data)
                          :updated          (:updated file-data)})))
  file-data)


(defn ^File create-file
  "Create and return a File object with the name based on the file key codec and base dir"
  [base-dir codec file-key]
  (File/createTempFile
    (str file-key (codec-extension codec) "_"),
    ""
    (io/file (select-base-dir base-dir))))

(defn do-create-file!
  "Create the parent directories and run create new file"
  [base-dir codec file-key]
  (try
    (.mkdirs (io/file base-dir))
    (create-file base-dir codec file-key)
    (catch Exception e (throw (ex-info (str e) {:cause e :base-dir base-dir :file-key file-key})))))

(defn- create-file-data!
  "Create a file and return a map with keys file codec file-key out,
   out contains the output stream and will always be a DataOutputStream
  Keys returned are: file, codec, file-key

  k: the key/topic provided on write
  conf: configuration while the ape context was created
  env: io plugin environment ref
  file-key: file key created based on k
  "
  [k {:keys [codec base-dir] :as conf} env file-key]
  (let [^File file (do-create-file! base-dir codec file-key)]

    (info "create new file " codec " " (.getAbsolutePath file))



    (when-not (.exists file) (throw (IOException. (str "Failed to create " file))))

    (assoc
      (get-output k file env conf)
      :file file
      :codec codec
      :file-key file-key
      :future-file-name (io/file (create-future-file-name file))
      :record-counter (AtomicLong. 0)
      :updated (AtomicReference. (System/currentTimeMillis)))))

(defn- create-agent [k conf env file-key]
  ;create an agent with a default mailbox-len of 10
  (fagent/agent (create-file-data! k conf env file-key) :mailbox-len 10))


(defn- create-if-not
  "Returns [(agent (delay file-data)) map]"
  [k conf env file-key m]
  (if-let [o (get m k)] (vector o m) (let [o (create-agent k conf env file-key)] (vector o (assoc m k o)))))

(defn- write-to-agent!
  "Calls the writer-f via the agent sending it the agents dereferenced result
   Error-handling: If any errors while envoking writer-f, a call is made to (:error-ch ctx) sending [error writer-f], (error e e) is also called"
  [ctx writer-f [agnt :as tpl]]
  {:pre [(:error-ch ctx)]}

  (fagent/send agnt (fn [file-desc] (try (writer-f file-desc) (catch Exception e
                                                                (do
                                                                  (error e e)
                                                                  (async/>!! (:error-ch ctx) [e writer-f])))) file-desc))
  tpl)

(defn- do-roll!
  "Helper function that calls roll-and-notify"
  [ctx file-desc]
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
                 (if (fagent/send agnt (partial do-roll! ctx))
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


(defn nth-one [coll]
  (nth coll 1))


(defn write-to-agent-helper
  "Helper function from agent-root-send-f that does:
      create-if-not agent
      write to agent, return result of nth-one"
  [k ctx file-key writer-f m]
  (->> m
       (create-if-not k
                      (:conf ctx)
                      (:env ctx)
                      file-key)

       (write-to-agent! ctx writer-f)

       nth-one))


(defn retry? [ctx tries]
  {:pre [(get-in ctx [:conf :retries])]}
  (< tries (get-in ctx [:conf :retries])))

(defn retry-sleep! [ctx]
  {:pre [(get-in ctx [:conf :retry-sleep])]}
  (Thread/sleep (long (get-in ctx [:conf :retry-sleep]))))

(defn agent-root-send
  "Called from the root agent
   Will retry on exception as long as retry? return true
   otherwise on exception it will call (>!! error-ch [exception writer-f])"
  [k ctx file-key writer-f tries m]
  (try
    (write-to-agent-helper k ctx file-key writer-f m)
    (catch Exception e (do
                         (info "Retry:  " k " " file-key " " e " tries " tries)

                         (if (retry? ctx tries)
                           (do
                             (retry-sleep! ctx)
                             (agent-root-send k ctx file-key writer-f (inc tries) m))
                           (do
                             ;;it is important that after doing error handling we return m
                             ;;otherwise the root agent will have a value other than expected e.g boolean(true)
                             (error e e)
                             (async/>!! (:error-ch ctx) [e writer-f])
                             m))))))

(defn async-write!
  "Creates a file based on the key k, the file descriptor is cached so that its only created once
   The descriptor is passed to the writer-f, and the final state is maintained in the (:state ctx)"
  [k ctx file-key writer-f]
  (if (open? ctx)
    (fagent/send (:root-agent ctx)
                 #(agent-root-send k ctx file-key writer-f 0 %))
    (throw (RuntimeException. "The writer context has already been closed"))))

(defn async-write-timeout!
  "Creates a file based on the key k, the file descriptor is cached so that its only created once
   The descriptor is passed to the writer-f, and the final state is maintained in the (:state ctx)"
  [k ctx file-key writer-f timeout]
  (if (open? ctx)
    (fagent/send-timeout
      (:root-agent ctx)
      #(agent-root-send k ctx file-key writer-f 0 %)
      timeout)
    (throw (RuntimeException. "The writer context has already been closed"))))


(defn shutdown! [ctx]
  (.set ^AtomicBoolean (:shutdown-flag ctx) true)
  (check-roll! ctx (fn [& _] true) :close-and-wait true)
  (fagent/close-agent (:root-agent ctx) :wait true))