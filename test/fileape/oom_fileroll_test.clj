(ns fileape.oom-fileroll-test
  "Test that the filedata is removed when roll conditions are met"
  (:import [java.io File DataOutputStream])
  (:require [midje.sweet :refer :all]
            [fileape.core :refer :all]))

(facts "Test key removal on roll condition"
       (fact "Test key removal"
             (let [base-dir (File. "target/tests/key-removal-test")
                   ape2 (ape {:codec :gzip :base-dir base-dir :check-freq 200 :rollover-timeout 200})]
               (dotimes [i 2]
                 (write ape2 (str "test-" i) (fn [{:keys [^DataOutputStream out]}]
                                               (prn "prn Writing")
                                               (.writeChars out "HI"))))

               (Thread/sleep 500)
               ;;we expect the file to have been rolled and the key removed from the star's key map
               (count (keys (:ch-map-view ape2))) => 0)))
