(ns fileape.conf-test
  (:require [midje.sweet :refer :all]
            [fileape.conf :as conf]))


(fact "Test override conf"
      (conf/get-conf :a {:a.base-dir 1 :base-dir 2} :base-dir nil) => 1
      (conf/get-conf :a {:base-dir 2} :base-dir nil) => 2)

(fact "Test conf no topic usage"
      (conf/get-conf {:base-dir 2} :base-dir) => 2)