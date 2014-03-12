package fileape;

import java.io.DataOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import clojure.lang.AFn;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import clojure.lang.RT;
import clojure.lang.Symbol;

/**
 * 
 * FileApe java interface.
 * 
 */
public class FileApeConnector {

	static {
		RT.var("clojure.core", "use").invoke(Symbol.create("fileape.core"));
	}

	/**
	 * 
	 * @param conf
	 * @return
	 */
	public static final Object create(Map<String, Object> conf) {

		List<Object> propvals = new ArrayList<Object>();

		for (Map.Entry<String, Object> entry : conf.entrySet()) {
			if (entry.getKey().equals("codec")) {
				propvals.add(Keyword.find("codec"));
				propvals.add(Keyword.find(entry.getValue().toString()));
			}else if (entry.getKey().equals("roll-callbacks")) {
				propvals.add(Keyword.find("roll-callbacks"));
				Object val = entry.getValue();
				
				if(val instanceof Collection)
					propvals.add(val);
				else
					propvals.add(PersistentVector.create(val));
				
			}else {
				propvals.add(Keyword.find(entry.getKey()));
				propvals.add(entry.getValue());
			}
		}

		return RT.var("fileape.core", "ape").invoke(
				PersistentArrayMap.createAsIfByAssoc(propvals.toArray()));
	}

	/**
	 * 
	 * @param connector
	 * @param key
	 *            Write to the file <key>-date
	 * @param writer
	 *            A function with a single argument of type
	 *            java.io.DataOutputStream
	 */
	public static final void write(Object connector, String key, Writer writer) {
		RT.var("fileape.core", "write").invoke(connector, key, writer);
	}

	public static final void write(Object connector, String key,
			final String val) {
		try {
			write(connector, key, val.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			RuntimeException rte = new RuntimeException(e);
			rte.setStackTrace(e.getStackTrace());
			throw rte;
		}
	}

	public static final void write(Object connector, String key,
			final byte[] bts) {
		write(connector, key, new Writer() {
			@Override
			public void write(DataOutputStream out) throws Exception {
				out.write(bts);
			}
		});
	}

	/**
	 * Closes the connectors
	 * 
	 * @param connector
	 */
	public static final void close(Object connector) {
		RT.var("fileape.core", "close").invoke(connector);
	}

	/**
	 * 
	 * Helper for writing
	 * 
	 */
	public static abstract class Writer extends AFn {

		public abstract void write(DataOutputStream out) throws Exception;

		@Override
		public Object invoke(Object arg) {

			try {
				write((DataOutputStream) arg);
			} catch (Exception e) {
				RuntimeException rte = new RuntimeException(e);
				rte.setStackTrace(e.getStackTrace());
				throw rte;
			}

			return null;
		}

	}

}
