package rivet.persistence.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import rivet.cluster.spark.Row;

import scala.Tuple2;

import static java.util.Arrays.stream;
import static rivet.util.Util.setting;


public class HBase {
	public static final byte[] DATA_COLUMN_FAMILY = stringToBytes("data");
	
	public static Cell newCell(ImmutableBytesWritable row, String column, String value) {
		return CellUtil.createCell(
				row.copyBytes(),
				DATA_COLUMN_FAMILY,
				stringToBytes(column),
				System.currentTimeMillis(),
				KeyValue.Type.Maximum.getCode(),
				stringToBytes(value));
	}
	
	public static Put newPut(ImmutableBytesWritable key, Row row) {
		Put put = new Put(key.copyBytes());
		row.forEach((k, v) -> {
			try{ put.add(newCell(key, k, v)); } 
			catch (IOException e) { e.printStackTrace(); }
		});
		return put;
	}
	
	public static Result newResult(ImmutableBytesWritable key, Row row) {
		List<Cell> cells = new ArrayList<>();
		row.forEach((k, v) -> cells.add(newCell(key, k, v)));
		return Result.create(cells);
	}
	
	public static Map<String, String> resultToStrings (Result r) {
		Map<String, String> res = new HashMap<>();
		res.put("word", bytesToString(r.getRow()));
		r.getFamilyMap(DATA_COLUMN_FAMILY).forEach(
				(x, y) -> res.put(bytesToString(x), bytesToString(y)));
		return res;
	}
	
	public static byte[] stringToBytes (String string) { return Bytes.toBytes(string); }
	public static String bytesToString (byte[] bytes) {	return Bytes.toString(bytes); }
	
	public static byte[] intToBytes (int i) { return Bytes.toBytes(i); }
	public static int bytesToInt (byte[] bytes) { return Bytes.toInt(bytes); }

	public static String ibwToString(ImmutableBytesWritable ibw) {return bytesToString(ibw.get());}
	public static ImmutableBytesWritable stringToIBW(String str) {return new ImmutableBytesWritable(stringToBytes(str));}

	@SafeVarargs
	public static Configuration newConf (Tuple2<String, String>...settings) {
		Configuration conf = HBaseConfiguration.create();
		stream(settings).forEach((setting) -> conf.set(setting._1, setting._2));
		return conf;
	}
	
	public static Configuration newConf (String tableName) {
		return newConf(
				setting(TableInputFormat.INPUT_TABLE, tableName));
	}
}
