(ns
  ^{:doc "Helper namespace related to configuration,
          all configuration options should have a default and a specific key that can override it

          e.g :base-dir
          default is :base-dir, but for key :a doing :a.base-dir can override it.
          "}
  fileape.conf)



(defn get-conf
  ([conf k]
    (get conf k nil))
  ([topic conf k]
    (get-conf topic conf k nil))
  ([topic conf k default-v]
    (if-let [v (get conf (keyword (str (name topic) "." (name k))))]
      v
      (get conf k default-v))))


