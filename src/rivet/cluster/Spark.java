package rivet.cluster;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import rivet.persistence.HBase;

public class Spark {
	
	public static Result firstOrError (List<Result> results) {
		if (results.isEmpty() || results.get(0).isEmpty()) 
			throw new IndexOutOfBoundsException("Result set is empty!");
		else
			return results.get(0);
	}
	
	public static ImmutableBytesWritable stringToIBW (String string) {
		return new ImmutableBytesWritable(HBase.stringToBytes(string));
	}
	public static String ibwToString (ImmutableBytesWritable ibw) {
		return HBase.bytesToString(ibw.get());
	}
}
