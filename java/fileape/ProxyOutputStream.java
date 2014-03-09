package fileape;

import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * 
 * 
 */
public class ProxyOutputStream extends DataOutputStream {

	final OutputStream out;

	public ProxyOutputStream(OutputStream out) {
		super(out);
		this.out = out;
	}

}
