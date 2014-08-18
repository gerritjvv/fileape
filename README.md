# fileape

Write data to files split by topic and rolled over on size or a timeout, files can be compressed using lzo, snappy or gzip

This allows the user to write data and have the api take care of splitting to data into files based on keys e.g. topic-datetime, and rollover the data
on a timeout or size.

## Usage

[![Clojars Project](http://clojars.org/fileape/latest-version.svg)](http://clojars.org/fileape)


### Java Version

Depends on java 1.7

### Version compatibility

For the function sent to the write function, before version 0.5.0 the argument was a single ^DataOutputStream out. From 0.5.0 and forward a map is passed to the function with the keys: out, future-file-name, file, codec, file-key.


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

(write ape2 "abc-123" (fn [{:keys [^DataOutputStream out]}]
                                                 (.writeInt out (int 1))))

;keys sent to the file write function above are
; out ^java.io.DataOutputStream
; future-file-name ^String the file name after rolling
; file ^java.io.File the file name that is being written
; codec ^clojure.lang.Keyword
; file-key the key used to write the data
; record-counter java.util.concurrent.atomic.AtomicLong (a helper function is provided see record-count)
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

      //to get the file data map use the write_data method

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

## Properties

<table border="0">
<tr><td><b>Name</b></td><td><b>Description</b></td></tr>
<tr><td>:codec</td><td>the code to use :gzip, :gzip-native, :snappy, :bzip2, :none</td></tr>
<tr><td>:base-dir</td><td>the base directory or directories to use, if a list is specified a directory will be randomly selected on file create</td></tr>
<tr><td>:rollover-size</td><td>size in bytes on which the file is rolled</td></tr>
<tr><td>:rollover-timeout</td><td>if no bytes where written for this period of time in milliseconds to the file its rolled</td></tr>
<tr><td>:check-freq</td><td>frequency in milliseconds in which the open files are checked</td></tr>
<tr><td>:parallel-files</td><td>this is a performance property, for each topic n=parallel-files files will be created.</td></tr>
<tr><td>:rollover-abs-timeout</td><td>A file is only open rollover-abs-timeout milliseconds, default is Long/MAX_VALUE</td></tr>
</table>



## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
