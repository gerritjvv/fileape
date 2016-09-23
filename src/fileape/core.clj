(ns
  ^{:doc
    "entry point namespace, for usage see https://github.com/gerritjvv/fileape

     (require '[fileape.core :refer :all])
     (import '[java.io File DataOutputStream])
     (defn callback-f [{:keys [file]}]
        (prn \"File rolled \" file))
        (def ape2 (ape {:codec :gzip
                        :base-dir \"testdir\"
                        :check-freq 5000
                        :rollover-size 134217728
                        :rollover-timeout 60000
                        :roll-callbacks [callback-f]}))
                        (write ape2 \"abc-123\" (fn [{:keys [^DataOutputStream out]}]
                                                    (.writeInt out (int 1))))
         (close ape2)
    "}

  fileape.core
  (:require
    [fileape.io :as io]
    [clojure.core.async :as async]
    [fun-utils.core :as fun-utils]
    [clojure.tools.logging :refer [info error]]
    [fileape.util.io-util :as io-util])
  (:import (java.util.concurrent ThreadLocalRandom)
           (java.io File)
           (java.util.concurrent.atomic AtomicReference)))


(defn th-rand-int
  "Returns a random number using the ThreadLocalRandom class from java 1.7"
  [^Long n]
  (-> (ThreadLocalRandom/current) (.nextLong n)))


(defn- create-parallel-key
  "Create a key name that is based on file-key + rand[0 - parallel-files]"
  [{:keys [parallel-files]} file-key]
  (str file-key "." (th-rand-int parallel-files)))

(defn roll-over-check [{:keys [rollover-size rollover-timeout rollover-abs-timeout]} {:keys [^File file ^AtomicReference updated]}]
  (let [tm-diff (- (System/currentTimeMillis) (.get updated))]
    (or (and (pos? (.length file)) (>= (.length file) rollover-size))
        (>= tm-diff rollover-timeout)
        (>= tm-diff rollover-abs-timeout))))


(defn write [ape k f]
  (let [ctx (:ctx ape)
        conf (:conf ctx)]
    (fileape.io-plugin/validate-env! k (:env ctx) conf)
    (io/async-write! k ctx (create-parallel-key conf k) f)))

(defn write-timeout
  "Write with timeout support, if the timeout is reached before a write can start a TimeoutException is thrown"
  [ape k f timeout]
  (let [ctx (:ctx ape)
        conf (:conf ctx)]
    (fileape.io-plugin/validate-env! k (:env ctx) conf)
    (io/async-write-timeout! k ctx (create-parallel-key conf k) f timeout)))

(defn close [ape]
  (fun-utils/stop-fixdelay (:fix-delay-ch ape))
  (io/shutdown! (:ctx ape)))


(defn update-env!
  "Update the :env variable for a ape context.
   Calls alter env f args,
   This is used to support any dynamic env plugin storage,

   Note that this env should be keyed on the same keys given to write"
  [{{:keys [env conf]} :ctx} k & args]
  {:pre [env k conf (:codec conf)]}
  (apply fileape.io-plugin/update-env! k env conf args))

(defn get-env
  "Returns the environment map (not the ref)"
  [{:keys [ctx]}]
  @(get ctx :env))

(defn get-codec [{:keys [ctx]}]
  (get-in ctx [:conf :codec]))

(defn get-base-dir [{:keys [ctx]}]
  (get-in ctx [:conf :base-dir]))

(defn ape-new-ctx
  "Create a new ape ctx with the k v applied to the config,
   usage: (ape-new-ctx ape :codec :lzo)"
  [{:keys [ctx] :as ape-ctx} k v]
  (assoc
    ape-ctx
    :ctx (io/update-ctx ctx (assoc (:conf ctx) k v))))

(defn ape
  "Entrypoint to the api, creates the resources for writing
   roll-callbacks - on each file roll all functions in this list are called, they are called with a map of keys [file codec file-key future-file-name ^AtomicLong record-count ^AtomicReference(long) updated]
   error-handler a function that is called if any errors happen in the writer agents as (error-handler error fn)
   Returns a map with a key :env that can be updated using update-env!, and is used depending on the storage plugins

   Retries: operations in the root agent i.e create-file and resources are retried (default 3) with a retry-sleep between each retry (default 500ms)
            it is done so that if file resources are scarce we try our best to wait then retry etc, if we run out of retries the error-handler is called
            Retries do block the root agent and thus all sending"
  [{:keys [codec base-dir rollover-size rollover-timeout rollover-abs-timeout check-freq roll-callbacks error-handler
           out-buffer-size use-buffer
           parallel-files
           retries
           retry-sleep
           env] :or {codec           :gzip check-freq 10000 rollover-size 134217728 rollover-timeout 60000 parallel-files 2 rollover-abs-timeout Long/MAX_VALUE
                     retries         3
                     retry-sleep     500
                     out-buffer-size 32768                  ;32KB
                     use-buffer      true} :as ape-conf}]
  {:pre [base-dir]}

  (let [error-ch (async/chan (async/sliding-buffer 10))
        roll-ch (async/chan (async/sliding-buffer (if (not-empty roll-callbacks) 100 1)))
        env-ref (ref (merge {} env))
        conf
        (merge ape-conf
               {:codec                codec
                :base-dir             base-dir
                :rollover-size        rollover-size
                :rollover-timeout     rollover-timeout
                :check-freq           check-freq
                :error-ch             error-ch
                :parallel-files       parallel-files
                :rollover-abs-timeout rollover-abs-timeout
                :roll-ch              roll-ch
                :out-buffer-size      out-buffer-size
                :use-buffer           use-buffer
                :retries              retries
                :retry-sleep          retry-sleep
                :file-data-create     (fn [& _])})
        ctx (io/create-ctx conf env-ref roll-ch error-ch)]

    (info "start file check " check-freq " rollover-sise " rollover-size " rollover-timeout " rollover-timeout)
    ;;if any error handlers
    (when error-handler
      (fun-utils/go-seq
        (fn [v]
          (try
            (apply error-handler v)
            (catch Exception e (error e e))))
        error-ch))

    ;if any rollbacks
    (when (not-empty roll-callbacks)
      (fun-utils/go-seq
        (fn [v]
          (try
            (->> roll-callbacks (map #(% v)) doall)
            (catch Exception e (error e e))))
        roll-ch))

    {:ctx          ctx
     :fix-delay-ch (fun-utils/fixdelay-thread check-freq (try
                                                           (io/check-roll! ctx (partial roll-over-check conf))
                                                           (catch Exception e (error e e))))}))


(defn create-future-file-name [f] (io/create-future-file-name f))
