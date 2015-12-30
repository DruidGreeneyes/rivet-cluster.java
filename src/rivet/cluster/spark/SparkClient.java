package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import rivet.Util;
import rivet.cluster.Spark;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.persistence.HBase;
import rivet.persistence.hbase.HBaseClient;

import scala.Tuple2;


public class SparkClient implements Closeable {
	private final int size = 16000;
	private final int k = 48;
	private final int cr = 3;
	
	private SparkConf sparkConf;
	private JavaSparkContext jsc;
	private Configuration conf;
	private JavaPairRDD<ImmutableBytesWritable, Result> rdd;
	
	private static byte[] DATA = HBase.stringToBytes("data");
	private static byte[] LEX = HBase.stringToBytes("lex");
	
	private static ImmutableBytesWritable META = Spark.stringToIBW("metadata");

	private static byte[] SIZE = HBase.stringToBytes("size");
	private static byte[] K = HBase.stringToBytes("k");
	private static byte[] CR = HBase.stringToBytes("cr");
	
	
	//Class necessities
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
	
	@Override
	public void close() throws IOException {
		this.rdd.saveAsNewAPIHadoopDataset(this.conf);
		this.jsc.stop();
		this.jsc.close();
	}
	
	
	//low-level
	public Optional<Result> getRow (ImmutableBytesWritable rowKey) {
		return Spark.firstOrError(this.rdd.lookup(rowKey));
	}
	
	public Optional<byte[]> getDataPoint (ImmutableBytesWritable rowKey, byte[] columnKey) {
		return this.getRow(rowKey)
				.map((x) -> x.getValue(DATA, columnKey));
	}
	
	public Optional<Result> setRow (ImmutableBytesWritable rowKey, Result newRes) {
		List<Tuple2<ImmutableBytesWritable, Result>> newRow = new ArrayList<>();
		newRow.add(new Tuple2<ImmutableBytesWritable, Result>(
				META, newRes));
		JavaPairRDD<ImmutableBytesWritable, Result> resRDD = jsc.parallelizePairs(newRow);
		this.rdd = rdd.union(resRDD).distinct();
		return this.getRow(rowKey);
	}
	
	public Optional<byte[]> setDataPoint (ImmutableBytesWritable rowKey, byte[] columnKey, byte[] value) {
		Optional<Result> r = this.getRow(rowKey);
		List<Cell> newCells = (r.isPresent())
				? r.get().getColumnCells(DATA, columnKey)
						: new ArrayList<>();
		if (newCells.isEmpty()) 
			System.out.println("Adding new Row: " + Spark.ibwToString(rowKey));
		newCells.add(CellUtil.createCell(
				rowKey.copyBytes(), 
				DATA, 
				columnKey, 
				System.currentTimeMillis(),
				KeyValue.Type.Maximum.getCode(),
				value));
		return this.setRow(rowKey, Result.create(newCells))
				.map((x) -> x.getValue(DATA, columnKey));
	}	
	
	
	//Lexicon-specific
	public Optional<Integer> getMetadata(byte[] item) {
		return this.getDataPoint(META, item).map(HBase::bytesToInt);
	}
	
	public Optional<Integer> setMetadata(byte[] item, int value) {
		return this.setDataPoint(META, DATA, HBase.intToBytes(value))
				.map(HBase::bytesToInt);
	}
	
	public int getSize () {	
		return this.getMetadata(SIZE).orElse(this.size);
	}
	public int getK () { 
		return this.getMetadata(K).orElse(this.k);
	}
	public int getCR () { 
		return this.getMetadata(CR).orElse(this.cr); 
	}
	public Optional<Integer> setSize (int size) {
		return this.setMetadata(SIZE, size);
	}
	public Optional<Integer> setK (int k) {
		return this.setMetadata(K, k);
	}
	public Optional<Integer> setCR (int cr) {
		return this.setMetadata(CR, cr);
	}
		
	public RIV getWordInd (String word) {
		 return HashLabels.generateLabel(
				 this.getSize(),
				 this.getK(),
				 word);
	}
	public Optional<RIV> getWordLex (String word) {
		return this.getDataPoint(Spark.stringToIBW(word), LEX).map(
				(x) -> RIV.fromString(HBase.bytesToString(x)));
	}
	
	public RIV getOrMakeWordLex (String word) {
		return this.getWordLex(word)
				.orElse(this.getWordInd(word));
	}

	public Optional<RIV> hbcSetWordLex (String word, RIV lex) throws IOException {
		try (HBaseClient hbc = new HBaseClient(this.conf)) {
			hbc.setWord(word, lex.toString());
		}
		return this.getWordLex(word);
	}
	
	public Optional<RIV> rddSetWordLex (String word, RIV lex) {
		this.setDataPoint(
				Spark.stringToIBW(word),
				LEX, 
				HBase.stringToBytes(lex.toString()));
		return this.getWordLex(word);
	}
	
	
	
	//Text Input
	public RIV trainWordFromContext (String word, JavaRDD<String> context) {
		return this.rddSetWordLex(
					word,
					context.map((x) -> this.getWordInd(x))
						   .fold(
								this.getOrMakeWordLex(word), 
								HashLabels::addLabels))
				.orElseThrow(IndexOutOfBoundsException::new);
	}
	
	public JavaPairRDD<Integer, String> index (JavaRDD<String> tokens) {
		return jsc.parallelize(
					Util.range((int)tokens.count()))
				.zip(tokens);
	}
	
	public void trainWordsFromText (JavaRDD<String> tokenizedText, int cr) {
		JavaPairRDD<Integer, String> tokens = this.index(tokenizedText);
		JavaRDD<Integer> indices = tokens.keys();
		tokens.foreach((entry) -> 
			this.trainWordFromContext(
				entry._2(),
				jsc.parallelize(Util.rangeNegToPos(cr))
					.map((x) -> x + entry._1())
					.intersection(indices)
					.map((x) -> tokens.lookup(x).get(0))));
	}
	public void trainWordsFromText (JavaRDD<String> tokenizedText) {
		trainWordsFromText(tokenizedText, this.getCR());
	}
	
	public void trainWordsFromBatch (JavaPairRDD<String, String> tokenizedTexts) {
		int cr = this.getCR();
		tokenizedTexts.values()
			.map((text) -> jsc.parallelize(
								Arrays.asList(text.split("\\s+"))))
			.foreach((tokens) -> this.trainWordsFromText(tokens, cr));
	}
	
	
	
	//Files
	public JavaRDD<String> loadTextFile (String path) {
		FlatMapFunction<String, String> brk = 
				(str) -> Arrays.asList(str.split("\\s+"));
		return jsc.textFile(path).flatMap(brk);
	}
	
	public JavaPairRDD<String, String> loadTextDir (String path) {
		return jsc.wholeTextFiles(path);
	}
}