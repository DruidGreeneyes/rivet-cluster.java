package rivet.persistence.hbase;

import rivet.persistence.HBase;

public class Delete extends org.apache.hadoop.hbase.client.Delete{
	public Delete(String row) { super(HBase.stringToBytes(row)); }
	public Delete(String row, String family, String column) { 
		this(row);
		this.addColumns(
				HBase.stringToBytes(family),
				HBase.stringToBytes(column));
	}
}
