package rivet.cluster;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import rivet.persistence.HBase;

public class Spark {
	
	public static Optional<Result> firstOrError (List<Result> results) {
		return (results.isEmpty() || results.get(0).isEmpty()) 
				? Optional.empty()
				: Optional.of(results.get(0));
	}
	
	public static ImmutableBytesWritable stringToIBW (String string) {
		return new ImmutableBytesWritable(HBase.stringToBytes(string));
	}
	public static String ibwToString (ImmutableBytesWritable ibw) {
		return HBase.bytesToString(ibw.get());
	}
}
