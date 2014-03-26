# fileape

Write data to files split by topic and rolled over on size or a timeout, files can be compressed using lzo, snappy or gzip 

This allows the user to write data and have the api take care of splitting to data into files based on keys e.g. topic-datetime, and rollover the data
on a timeout or size.

## Usage

```[fileape "0.4.4-SNAPSHOT"]```

```clojure

(require '[fileape.core :refer :all])
(import '[java.io File DataOutputStream])

(defn callback-f [{:keys [file]}]
   (prn "File rolled " file))

(def ape2 (ape {:codec :gzip
		:base-dir "testdir" 
                :check-freq 5000
                :rollover-size 134217728
                :rollover-timeout 60000
                :roll-callbacks [callback-f]}))

(write ape2 "abc-123" (fn [^DataOutputStream o] 
                                                 (.writeInt o (int 1))))


(close ape2)
               
```


### Java

```java
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Map;

import clojure.lang.AFn;
import clojure.lang.Keyword;
import fileape.FileApeConnector;
import fileape.FileApeConnector.Writer;

public class Test {

	
	public static final void main(String[] args){
		
		final Map<String, Object> conf = new HashMap<String, Object>();
		
		conf.put("base-dir", "/tmp/test");
		conf.put("codec", "gzip");
		conf.put("check-freq", 5000);
		conf.put("rollover-size", 134217728);
		conf.put("rollover-timeout", 60000);
		conf.put("roll-callbacks", new AFn() {

			@SuppressWarnings("unchecked")
			@Override
			public Object invoke(Object arg) {
				Map<Object, Object> fileData = (Map<Object, Object>)arg;
				System.out.println("File rolled: " + fileData.get(Keyword.find("file")));
				return null;
			}
			
		});
		
		//create the connector
		final Object connector = FileApeConnector.create(conf);
		
		//using a string
		FileApeConnector.write(connector, "test", "Hi\n");
		//using bytes
		FileApeConnector.write(connector, "test", "Hi\n".getBytes());
		//using a callback function
		FileApeConnector.write(connector, "test", new Writer(){

			@Override
			public void write(DataOutputStream out) throws Exception {
				out.writeChars("Hi\n");
			}
			
		});
		
		FileApeConnector.close(connector);
		
	}
	

```

## LZO 

LZO is distributed under the GPL 3 license and therefore fileape needs to come in two versions fileape (apache2) and fileape-lzo (gpl).

The api is exactly the same with the difference to the license and the extra libraries required for lzo compression.

For more information on setting up lzo correctly see: https://code.google.com/p/hadoop-gpl-packing/

```[fileape-lzo "0.4.4-SNAPSHOT"]```

```clojure

(require '[fileape.core :refer :all])
(import '[java.io File DataOutputStream])

(defn callback-f [{:keys [file]}]
   (prn "File rolled " file))

(def ape2 (ape {:codec :lzo
		:base-dir "testdir" 
                :check-freq 5000
                :rollover-size 134217728
                :rollover-timeout 60000
                :roll-callbacks [callback-f]}))

(write ape2 "abc-123" (fn [^DataOutputStream o] 
                                                 (.writeInt o (int 1))))


(close ape2)
               
``` 

## License


Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
