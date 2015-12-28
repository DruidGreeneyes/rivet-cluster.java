package rivet.persistence.hbase;

import rivet.persistence.HBase;

public class Get extends org.apache.hadoop.hbase.client.Get {
	public Get(String row) { super(HBase.stringToBytes(row)); }
}
