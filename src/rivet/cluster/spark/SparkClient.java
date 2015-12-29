package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import rivet.Util;
import rivet.cluster.Spark;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.persistence.HBase;
import rivet.persistence.hbase.HBaseClient;

import scala.Tuple2;


public class SparkClient implements Closeable {
	private SparkConf sparkConf;
	private JavaSparkContext jsc;
	private Configuration conf;
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

	public RIV hbcSetWordLex (String word, RIV lex) throws IOException {
		try (HBaseClient hbc = new HBaseClient(this.conf)) {
			hbc.setWord(word, lex.toString());
		}
		return this.getWordLex(word);
	}
	
	public RIV rddSetWordLex (String word, RIV lex) {
		ImmutableBytesWritable wordBytes = Spark.stringToIBW(word);
		Result oldRow = this.getDataPoint(wordBytes);
		Cell oldCell = oldRow.getColumnLatestCell(DATA, LEX);
		List<Cell> newCells = Util.safeCopy(oldRow.listCells());
		newCells.add(CellUtil.createCell(
				oldCell.getRowArray(),
				oldCell.getFamilyArray(),
				oldCell.getQualifierArray(),
				System.currentTimeMillis(),
				KeyValue.Type.Maximum.getCode(),
				HBase.stringToBytes(lex.toString())));
		List<Tuple2<ImmutableBytesWritable, Result>> newRow = new ArrayList<>();
		newRow.add(new Tuple2<ImmutableBytesWritable, Result>(
				wordBytes, Result.create(newCells)));
		JavaPairRDD<ImmutableBytesWritable, Result> resRDD = jsc.parallelizePairs(newRow);
		this.rdd = rdd.union(resRDD).distinct();		
		return this.getWordLex(word);
	}


	@Override
	public void close() throws IOException {
		this.rdd.saveAsNewAPIHadoopDataset(this.conf);
		this.jsc.stop();
		this.jsc.close();
	}
}