;;see https://github.com/gerritjvv/fileape for usage
(ns fileape.core
  (require
    [fileape.io :as io]
    [clojure.core.async :as async]
    [fun-utils.core :as fun-utils]
    [clojure.tools.logging :refer [info error]])
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
    (or (>= (.length file) rollover-size)
        (>= tm-diff rollover-timeout)
        (>= tm-diff rollover-abs-timeout))))


(defn write [ape k f]
  (io/async-write! (:ctx ape) (create-parallel-key (get (get ape :ctx) :conf) k) f))

(defn close [ape]
  (fun-utils/stop-fixdelay (:fix-delay-ch ape))
  (io/shutdown! (:ctx ape)))

(defn ape
  "Entrypoint to the api, creates the resources for writing"
  [{:keys [codec base-dir rollover-size rollover-timeout rollover-abs-timeout check-freq roll-callbacks
           parallel-files] :or {codec :gzip check-freq 10000 rollover-size 134217728 rollover-timeout 60000 parallel-files 3 rollover-abs-timeout Long/MAX_VALUE}}]
  (let [error-ch (async/chan (async/sliding-buffer 10))
        roll-ch (async/chan (async/sliding-buffer 100))
        conf
        {:codec            codec
         :base-dir         base-dir
         :rollover-size    rollover-size
         :rollover-timeout rollover-timeout
         :check-freq       check-freq
         :error-ch         error-ch
         :parallel-files   parallel-files
         :rollover-abs-timeout rollover-abs-timeout
         :roll-ch          roll-ch
         :file-data-create (fn [& _])}
        ctx (io/create-ctx conf roll-ch)]

    (info "start file check " check-freq " rollover-sise " rollover-size " rollover-timeout " rollover-timeout)
    ;if any rollbacks
    (when (not-empty roll-callbacks)
      (fun-utils/go-seq
        (fn [v]
          (try
            (->> roll-callbacks (map #(% v)) doall)
            (catch Exception e (prn e e))))
        roll-ch))

    {:ctx ctx
     :fix-delay-ch (fun-utils/fixdelay-thread check-freq (try
                                                           (io/check-roll! ctx (partial roll-over-check conf))
                                                           (catch Exception e (error e e))))}))


(defn create-future-file-name [f] (io/create-future-file-name f))