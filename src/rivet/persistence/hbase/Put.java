package rivet.persistence.hbase;

import java.util.Map;

import rivet.persistence.HBase;

public class Put extends org.apache.hadoop.hbase.client.Put {
	public Put(String row) { super(HBase.stringToBytes(row)); }
	public Put(Map<String, String> data) {
		this(data.get("word"));
		data.forEach((k, v) -> {
					if (!k.equalsIgnoreCase("word"))
						this.addColumn(
								HBase.dataColumn,
								HBase.stringToBytes(k),
								HBase.stringToBytes(v));
		});
	}
}
