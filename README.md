# fileape

Write data to files split by topic and rolled over on size or a timeout, files can be compressed using lzo, snappy or gzip 

This allows the user to write data and have the api take care of splitting to data into files based on keys e.g. topic-datetime, and rollover the data
on a timeout or size.

## Usage

```[fileape "0.3.0-SNAPSHOT"]```

```clojure

(require '[fileape.core :refer :all])
(import '[java.io File DataOutputStream])

(def ape2 (ape {:codec :gzip}))
(write ape2 "abc-123" (fn [^DataOutputStream o] 
                                                 (.writeInt o (int 1))))

(close ape2)
               
```

## License


Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
