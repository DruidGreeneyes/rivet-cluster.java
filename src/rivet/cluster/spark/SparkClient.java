package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import rivet.cluster.Spark;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.persistence.HBase;


public class SparkClient implements Closeable {
	private SparkConf sparkConf;
	private JavaSparkContext jsc;
	private Configuration conf;
	private Connection conn;
	private Admin admin;
	private TableName tn;
	private JavaPairRDD<ImmutableBytesWritable, Result> rdd;
	
	private static byte[] DATA = HBase.stringToBytes("data");
	private static byte[] LEX = HBase.stringToBytes("lex");
	
	private static ImmutableBytesWritable IMETA = Spark.stringToIBW("(:meta");
	
	private static byte[] META = HBase.stringToBytes("(:meta");
	private static byte[] SIZE = HBase.stringToBytes("size");
	private static byte[] K = HBase.stringToBytes("k");
	private static byte[] CR = HBase.stringToBytes("cr");
	
	public SparkClient (String tableName) throws IOException {
		this.sparkConf = new SparkConf().setAppName("Rivet");
		this.jsc = new JavaSparkContext(sparkConf);
		this.conf = HBaseConfiguration.create();
		this.conf.set(TableInputFormat.INPUT_TABLE, tableName);
		this.conn = ConnectionFactory.createConnection(this.conf);
		this.admin = this.conn.getAdmin();
		this.tn = TableName.valueOf(tableName);
		this.rdd = jsc.newAPIHadoopRDD(
						conf,
						TableInputFormat.class,
						ImmutableBytesWritable.class, 
						Result.class);
	}
	
	public int getMetadata(byte[] item) {
		return HBase.bytesToInt(
				Spark.firstOrError(
						this.rdd.lookup(IMETA))
					.getColumnLatestCell(META, item)
					.getValueArray());
	}
	public int getSize () {	return this.getMetadata(SIZE); }
	public int getK () { return this.getMetadata(K); }
	public int getCR () { return this.getMetadata(CR); }
		
	public RIV getWordInd (String word) {
		 return HashLabels.generateLabel(
				 this.getSize(),
				 this.getK(),
				 word);
	}
	
	public Result getDataPoint (ImmutableBytesWritable row) {
		return Spark.firstOrError(this.rdd.lookup(row));
	}
	public byte[] getDataPoint (ImmutableBytesWritable row, byte[] column) {
		return this.getDataPoint(row)
				.getColumnLatestCell(DATA, column)
				.getValueArray();
	}
	public byte[] getDataPoint (String row, byte[] column) {
		return this.getDataPoint(Spark.stringToIBW(row), column);
	}
	public byte[] getDataPoint (ImmutableBytesWritable row, String column) {
		return this.getDataPoint(row, HBase.stringToBytes(column));
	}
	public byte[] getDataPoint (String row, String column) {
		return this.getDataPoint(row, HBase.stringToBytes(column));
	}
	
	public RIV getWordLex (String word) {
		return RIV.fromString(
				HBase.bytesToString(
					this.getDataPoint(word, LEX)));
	}
	
	public RIV getOrMakeWordLex (String word) {
		try {
			return this.getWordLex(word);
		} catch (IndexOutOfBoundsException e) {
			return this.getWordInd(word);
		}
	}
	
/*
	public RIV setWordLex (String word, RIV lex) {
		Result w = this.getDataPoint(Spark.stringToIBW(word));
		rdd.
	}
*/

	@Override
	public void close() throws IOException {
		admin.flush(tn);
		admin.close();
		conn.close();
		jsc.stop();
		jsc.close();
	}
}