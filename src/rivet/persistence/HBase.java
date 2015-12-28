package rivet.persistence;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase {
	public static byte[] dataColumn = stringToBytes("data");
	
	public static Map<String, String> resultToStrings (Result r) {
		Map<String, String> res = new HashMap<>();
		res.put("word", bytesToString(r.getRow()));
		r.getFamilyMap(dataColumn).forEach(
				(x, y) -> res.put(bytesToString(x), bytesToString(y)));
		return res;
	}
	public static byte[] stringToBytes (String string) { return Bytes.toBytes(string); }
	public static String bytesToString (byte[] bytes) {	return Bytes.toString(bytes); }
}
