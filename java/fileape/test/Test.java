package fileape.test;

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
	
}
