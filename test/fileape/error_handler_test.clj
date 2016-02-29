(ns fileape.error-handler-test
  (:require [fileape.core :as ape]
            [midje.sweet :refer :all])
  (:import (java.io File)))

;;;;;;;;;;;;;;;;;;;;;;;
;;;; helper functions

(defn ^File temp-dir []
  (let [file (File. (str "target/test/error-handler-test/" (System/currentTimeMillis)))]
    (.mkdirs file)
    file))


(defn ^File read-only-dir []
  (doto
    (temp-dir)
    (.setReadOnly)))

(defn test-file-ctx
  ([error-handler]
    (test-file-ctx error-handler (temp-dir)))
  ([error-handler dir]
   (ape/ape {:base-dir dir :error-handler error-handler})))

(defn wait-till-not-nil [at timeout]
  (let [ts (System/currentTimeMillis)
        test-not-done (fn [] (and (nil? @at) (< (- (System/currentTimeMillis) ts) timeout)))]

    ;;pause till either at is not nil or the timeout has expired
    (while (test-not-done)
      (Thread/sleep 100))

    (not (test-not-done))))


(defn always-error [& _] (throw (RuntimeException. "Test")))

(defn check-errors-called [errors]
  (when (wait-till-not-nil errors 10000)
    (let [[error f] (first @errors)]
      (and
        (instance? Exception error)
        (fn? f)))))

;;;;;;;;;;;;;;;;;;;;;;
;;;; test functions

(defn test-receive-errors
  "Test that the error handler is called when an exception is thrown"
  []
  (let [errors (atom nil)
        error-handler (fn [& args] (swap! errors conj args))

        ctx (test-file-ctx error-handler)]

    (ape/write ctx :a always-error)

    (check-errors-called errors)))

(defn test-retry-errors
  "Test that the retry config is used and we retry on error"
  []
  (let [errors (atom nil)
        error-handler (fn [& args] (swap! errors conj args))

        ctx (test-file-ctx error-handler (read-only-dir))]

    (ape/write ctx "abc" always-error)
    (check-errors-called errors)))

;;;;;;;;;;;;;;;;;;;;;
;;;; test handles

(facts "test-error-handler-is-called"
       (test-receive-errors) => true)

(facts "test-error-handler-is-called-after-retry"
       (test-receive-errors) => true)
