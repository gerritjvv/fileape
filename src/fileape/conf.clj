(ns
  ^{:doc "Helper namespace related to configuration,
          all configuration options should have a default and a specific key that can override it

          e.g :base-dir
          default is :base-dir, but for key :a doing :a.base-dir can override it.
          "}
  fileape.conf
  (:require [clojure.tools.logging :refer [debug]]))



(defn get-conf
  ([conf k]
    (get conf k nil))
  ([topic conf k]
    (get-conf topic conf k nil))
  ([topic conf k default-v]
    (let [kw-k (keyword (str (name topic) "." (name k)))]
      (if-let [v (get conf kw-k)]
        (do
          (debug "get-conf " kw-k " => " v)
          v)
        (let [v (get conf k default-v)]
          (debug "get-conf specific conf for " kw-k " not found using global config " k " => " v)
          v)))))


