package rivet.cluster.spark;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import org.apache.hadoop.hbase.client.Result;

import rivet.persistence.hbase.HBase;

public class Row extends TreeMap<String, String> {
	public Row(){super();}
	public Row(Row row) {super(row);}
	public Row(Map<String, String> map) {super(map);}
	public Row(Result result) {
		this();
		result.getFamilyMap(HBase.DATA_COLUMN_FAMILY).forEach(
				(k, v) -> this.put(
						HBase.bytesToString(k),
						HBase.bytesToString(v)));
	}
	
	public static Function<Row, String> getter (String key) {
		return (row) -> row.get(key);
	}
	
	public <T> T engage (Function<Row, T> fun) { return fun.apply(this); }

	/**
	 * 
	 */
	private static final long serialVersionUID = -8268389694010515839L;
	
}
