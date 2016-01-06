package rivet.persistence.hbase;

import java.util.Map;

import rivet.persistence.HBase;

public class Put extends org.apache.hadoop.hbase.client.Put {
	public Put(String row) { super(HBase.stringToBytes(row)); }
	public Put(Map<String, String> data, String rowKey) {
		this(data.get(rowKey));
		data.forEach((k, v) -> {
			if (!k.equalsIgnoreCase(rowKey))
				this.addColumn(
						HBase.dataColumn,
						HBase.stringToBytes(k),
						HBase.stringToBytes(v));
		});
	}
	public Put(Map<String, String> data) { this(data, "word"); }
}
