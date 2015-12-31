package rivet.cluster;

import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import rivet.persistence.HBase;

public class Spark {
	public static Cell retrieveCellByColumn (List<Cell> cells, byte[] columnKey) {
		return cells.stream()
				.filter((y) ->
					y.getQualifierArray() == columnKey)
				.max((c1, c2) -> 
					Long.compare(c1.getTimestamp(), c2.getTimestamp()))
				.get();
	}
	
	public static byte[] retrieveCellValueByColumn (List<Cell> cells, byte[] columnKey) {
		return retrieveCellByColumn(cells, columnKey).getValueArray();
	}

	public static String ibwToString(ImmutableBytesWritable ibw) {
		return HBase.bytesToString(ibw.get());
	}
	public static ImmutableBytesWritable stringToIBW(String str) {
		return new ImmutableBytesWritable(HBase.stringToBytes(str));
	}
}
