(ns
    fileape.test-app
    (:import (java.util.concurrent Executors))
    (:require [fileape.core :as core])
    :gen-class)


(defn start-test [ape [threads files]]
      (let [service (Executors/newCachedThreadPool)
            topics ()]
           ))

(defn -main [& args]
      (let [ape (core/ape :base-dir (first args) :rollover-size 1024)]
           (start-test ape (rest args))))

