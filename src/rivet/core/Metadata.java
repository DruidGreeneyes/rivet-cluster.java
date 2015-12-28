package rivet.core;

import java.util.HashMap;

@SuppressWarnings("serial")
public class Metadata extends HashMap<String, Integer> {
	private final String ss = ".size";
	private final String kk = ".k";
	private final String cc = ".cr";
	
	public Metadata (int size, int k, int cr) {
		this.setSize(size);
		this.setK(k);
		this.setCR(cr);
	}
	
	public int getSize () { return this.get(ss); }
	public int getK ()    { return this.get(kk); }
	public int getCR ()   { return this.get(cc); }
	
	
	private void setSize (int size) { this.put(ss, size); }
	private void setK (int k)       { this.put(kk, k); }
	private void setCR (int cr)     { this.put(cc, cr); }
}
