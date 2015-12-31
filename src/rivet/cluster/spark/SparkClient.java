package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

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
	
	private final byte KV_MAX = Byte.parseByte("-1");
	
	private final PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, NavigableMap<String, String>> B2BA = 
			(x) -> {
				NavigableMap<String, String> cells = new TreeMap<>();
				x._2.getFamilyMap(HBase.stringToBytes(DATA)).forEach((k, v) -> cells.put(HBase.bytesToString(k),
																	HBase.bytesToString(v)));
				return Tuple2.apply(Spark.ibwToString(x._1), cells);
			};
	@SuppressWarnings("unused")
	private final PairFunction<Tuple2<String, NavigableMap<String, String>>, ImmutableBytesWritable, Result> BA2B =
			(x) -> {
				List<Cell> cells = new ArrayList<>();
				long ts = System.currentTimeMillis();
				byte[] rowKey = HBase.stringToBytes(x._1);
				x._2.forEach((c, v) -> 
					cells.add(CellUtil.createCell(
							rowKey,
							HBase.stringToBytes(DATA), 
							HBase.stringToBytes(c), 
							ts,
							KV_MAX,
							HBase.stringToBytes(v))));
				return Tuple2.apply(new ImmutableBytesWritable(rowKey), Result.create(cells));
			};
	
	public SparkConf sparkConf;
	public JavaSparkContext jsc;
	public Configuration conf;
	public JavaPairRDD<String, NavigableMap<String, String>> rdd;
	
	public static String DATA = "data";
	public static String LEX = "lex";
	
	public static String META = "metadata";

	public static String SIZE = "size";
	public static String K = "k";
	public static String CR = "cr";
	
	
	//Class necessities
	public SparkClient (String tableName) throws IOException {
		this.sparkConf = new SparkConf().setAppName("Rivet").setMaster("local[2]");
		this.jsc = new JavaSparkContext(sparkConf);
		this.conf = HBaseConfiguration.create();
		this.conf.set(TableInputFormat.INPUT_TABLE, tableName);
		this.rdd = jsc.newAPIHadoopRDD(
						conf,
						TableInputFormat.class,
						ImmutableBytesWritable.class, 
						Result.class)
					.mapToPair(B2BA);
	}
	
	@Override
	public void close() throws IOException {
		this.jsc.stop();
		this.jsc.close();
	}
	
	
	//low-level
	public Optional<NavigableMap<String, String>> getRow (String rowKey) {
		List<NavigableMap<String, String>> res = this.rdd.lookup(rowKey);
		return (res.isEmpty() || res.get(0).isEmpty())
				? Optional.empty()
						: Optional.of(res.get(0));
	}
	
	public Optional<String> getDataPoint (String rowKey, String columnKey) {
		return this.getRow(rowKey)
				.map((cells) -> cells.get(columnKey));
	}
	
	public Optional<NavigableMap<String, String>> rddSetRow (String rowKey, NavigableMap<String, String> newRes) {
		List<Tuple2<String, NavigableMap<String, String>>> newRow = 
				new ArrayList<>();
		newRow.add(
				new Tuple2<String, NavigableMap<String, String>>(DATA, newRes));
		JavaPairRDD<String, NavigableMap<String, String>> resRDD = 
				jsc.parallelizePairs(newRow);
		this.rdd = rdd.union(resRDD).distinct();
		return this.getRow(rowKey);
	}
	public Optional<NavigableMap<String, String>> hbcSetRow (String rowKey, NavigableMap<String, String> newRes) throws IOException {
		NavigableMap<String, String> row = Util.safeCopy(newRes);
		row.put("word", rowKey);
		try (HBaseClient hbc = new HBaseClient(this.conf)) {
			hbc.put(row);
		}
		return this.getRow(rowKey);
	}
	
	public Optional<String> rddSetDataPoint (String row, String column, String value) {
		NavigableMap<String, String> newCells = Util.safeCopy(this.getRow(row)
															.orElse(new TreeMap<>()));
		if (newCells.isEmpty()) System.out.println("Adding new Row: " + row);
		newCells.put(column, value);
		return this.rddSetRow(row, newCells)
				.map((cells) -> cells.get(column));
	}
	public Optional<String> hbcSetDataPoint (String row, String column, String value) throws IOException {
		NavigableMap<String, String> map = new TreeMap<>();
		map.put(column, value);
		return this.hbcSetRow(row, map)
				.map((cells) -> cells.get(column));
	}
	
	
	//Lexicon-specific
	public Optional<Integer> getMetadata(String item) {
		return this.getDataPoint(META, item).map(Integer::parseInt);
	}
	
	public Optional<Integer> setMetadata(String item, int value) throws IOException {
		return this.hbcSetDataPoint(META, DATA, Integer.toString(value))
				.map(Integer::parseInt);
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
	public Optional<Integer> setSize (int size) throws IOException {
		return this.setMetadata(SIZE, size);
	}
	public Optional<Integer> setK (int k) throws IOException {
		return this.setMetadata(K, k);
	}
	public Optional<Integer> setCR (int cr) throws IOException {
		return this.setMetadata(CR, cr);
	}
		
	public RIV getWordInd (String word) {
		 return HashLabels.generateLabel(
				 this.getSize(),
				 this.getK(),
				 word);
	}
	public Optional<RIV> getWordLex (String word) {
		return this.getDataPoint(word, LEX).map(
				(x) -> RIV.fromString(x));
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
		this.rddSetDataPoint(
				word,
				LEX, 
				lex.toString());
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
	
	//Static
	public static byte[] sToBytes (String s) { return HBase.stringToBytes(s); }
	public static String bToString (byte[] b) { return HBase.bytesToString(b); }	
}