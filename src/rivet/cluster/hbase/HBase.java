package rivet.cluster.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

import rivet.cluster.spark.Row;

import scala.Tuple2;
import static java.util.Arrays.stream;
import static rivet.cluster.util.Util.setting;


public class HBase {
	public static final byte[] DATA_COLUMN_FAMILY = stringToBytes("data");
	
	private static HTableDescriptor newTableDescriptor(TableName name) {
		return new HTableDescriptor(name)
				.addFamily(
						new HColumnDescriptor(
								DATA_COLUMN_FAMILY));
	}
	
	public static boolean tableExists(String tableName) throws IOException {
		try (Connection conn = ConnectionFactory.createConnection()) {
			Admin admin = conn.getAdmin();
			TableName tn = TableName.valueOf(tableName);
			if (admin.tableExists(tn)) return true;
			System.out.format("Table '%s' does not exist. Create it? (y/n)" , tableName);
			String input = System.console().readLine().toLowerCase();
			if (input == "n") return false;
			admin.createTable(newTableDescriptor(tn));
			return true;
		}
	}
	
	public static boolean clearTable(String tableName) throws IOException {
		//log.log("Clearing table: " + tableName);
		try (Connection conn = ConnectionFactory.createConnection()) {
			Admin admin = conn.getAdmin();
			TableName tn = TableName.valueOf(tableName);
			System.out.println("Disabling table: " + tableName);
			admin.disableTable(tn);
			System.out.println("Deleting table: " + tableName);
			admin.deleteTable(tn);
			System.out.println("Creating new table: " + tableName);
			admin.createTable(newTableDescriptor(tn));
			return tableExists(tableName);
		}
	}
	
	public static Cell newCell(ImmutableBytesWritable row, String column, String value) {
		return CellUtil.createCell(
				row.get(),
				DATA_COLUMN_FAMILY,
				stringToBytes(column),
				System.currentTimeMillis(),
				KeyValue.Type.Maximum.getCode(),
				stringToBytes(value));
	}
	
	public static Put newPut(ImmutableBytesWritable key, Row row) {
		Put put;
		try {
			put = new Put(key.get());
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("Cannot create Put with arg: '" + key.toString() + "'");
		}
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
