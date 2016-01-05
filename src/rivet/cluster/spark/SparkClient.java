package rivet.cluster.spark;

import java.io.Closeable;
import java.io.Serializable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import rivet.persistence.hbase.HBaseConf;
import rivet.persistence.hbase.HadoopConf;

import scala.Tuple2;


public class SparkClient implements Closeable, Serializable {	
	private static final long serialVersionUID = -3403510942281426251L;
	/**
	 * 
	 */
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
	
	public JavaSparkContext jsc;
	public JavaPairRDD<String, NavigableMap<String, String>> rdd;
	
	public static String DATA = "data";
	public static String LEX = "lex";
	
	public static String META = "metadata";

	public static String SIZE = "size";
	public static String K = "k";
	public static String CR = "cr";
	
	
	//Class necessities
	public SparkClient (String tableName, String master, String hostRam, String workerRam) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("Rivet")
				.setMaster(master)
				.set("spark.driver.memory", hostRam)
				.set("spark.executor.memory", workerRam);
		this.jsc = new JavaSparkContext(sparkConf);
		this.rdd = this.loadHBaseTable(tableName);
	}
		
	public SparkClient (String tableName) throws IOException {
		this(tableName, "local[2]", "4g", "3g");
	}
	
	@Override
	public void close() throws IOException {
		this.jsc.close();
	}
	
	
	//low-level
	public JavaPairRDD<String, NavigableMap<String, String>> loadHBaseTable (String table) {
		return this.jsc.newAPIHadoopRDD(
								newConf(table),
								TableInputFormat.class,
								ImmutableBytesWritable.class, 
								Result.class)
							.mapToPair(B2BA)
							.setName(table);
	}
	
	public Optional<NavigableMap<String, String>> getRow (String rowKey, JavaPairRDD<String, NavigableMap<String, String>> rdd) {
		List<NavigableMap<String, String>> res = rdd.lookup(rowKey);
		return (res.isEmpty() || res.get(0).isEmpty())
				? Optional.empty()
						: Optional.of(res.get(0));
	}
	public Optional<NavigableMap<String, String>> getRow (String rowKey) {
		return this.getRow(rowKey, this.rdd);
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
				this.jsc.parallelizePairs(newRow);
		this.rdd = rdd.union(resRDD).distinct();
		return this.getRow(rowKey);
	}
	public Optional<NavigableMap<String, String>> hbcSetRow (String rowKey, NavigableMap<String, String> newRes) throws IOException {
		NavigableMap<String, String> row = new TreeMap<>(newRes);
		row.put("word", rowKey);
		try (HBaseClient hbc = new HBaseClient(rddConf(this.rdd))) {
			hbc.put(row);
		}
		return this.getRow(rowKey);
	}
	
	public boolean deleteRow (String row, JavaPairRDD<String, NavigableMap<String, String>> rdd) throws IOException {
		try (HBaseClient hbc = new HBaseClient(rddConf(rdd))) {
			hbc.delete(row);
		}
		return !this.getRow(row, rdd).isPresent();
	}
	public boolean deleteRow (String row) throws IOException {
		return this.deleteRow(row, this.rdd);
	}
	
	public Optional<String> rddSetDataPoint (String row, String column, String value) {
		NavigableMap<String, String> newCells = new TreeMap<>();
		this.getRow(row).ifPresent(newCells::putAll);
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
		System.out.println("getWordInd called with arg: " + word); 
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
		try (HBaseClient hbc = new HBaseClient(rddConf(this.rdd))) {
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

	public RIV trainWordFromContext (String word, List<String> context) throws IOException {
		System.out.println("trainWordFromContext called with args:\nWord: " + word +
							"\nContext: " + context.toString());
		RIV lex = context.stream()
					.map(this::getWordInd)
					.reduce(
						this.getOrMakeWordLex(word),
						HashLabels::addLabels);
		return this.hbcSetWordLex(word, lex)
				.orElseThrow(IndexOutOfBoundsException::new);
	}
	
	public Map<Long, String> index (List<String> tokens, Long count) {
		Map<Long, String> res = new HashMap<>();
		Util.range(count).forEach((i) -> 
			res.put(i, tokens.get(i.intValue())));
		return res;
	}
	
	public <T> JavaPairRDD<Long, T> index (JavaRDD<T> tokens, Long count) {
		return tokens.zipWithIndex().mapToPair(Tuple2::swap);
		
		
/*		
		List<Long> indices = Util.range(count); 
		JavaRDD<Long> rdd = this.jsc.parallelize(indices);
		Long rCount = rdd.count();
		System.out.println(count);
		System.out.println(indices.size() + " - " + indices.toString());
		System.out.println(rCount);
		JavaPairRDD<Long, T> res = rdd.zip(tokens);
		System.out.println(res.count() + " - " + res.keys().collect().toString());
		return res;
*/
	}

	public void trainWordsFromText (List<String> tokenizedText, Integer cr) throws IOException {
		Integer count = tokenizedText.size();
		Map<Long, String> tokens = this.index(tokenizedText, count.longValue());
		tokens.forEach((x, y) -> System.out.println(x + ": " + y));
		tokens.forEach((index, value) -> {
			try {
				this.trainWordFromContext(
						value,
						Util.butCenter(
								Util.rangeNegToPos(cr),
								cr)
							.stream()
							.map((x) -> tokens.get(x + index))
							.filter((x) -> x != null)
							.collect(Collectors.toList()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
	
	public void trainWordsFromText (List<String> tokenizedText) throws IOException {
		this.trainWordsFromText(tokenizedText, this.getCR());
	}
	
	/*
	public RIV trainWordFromContext (String word, JavaRDD<String> context) throws IOException {
		RIV lex = context.map(this::getWordInd)
					.fold(
						this.getOrMakeWordLex(word),
						HashLabels::addLabels);
		return this.hbcSetWordLex(word, lex)
				.orElseThrow(IndexOutOfBoundsException::new);
	}
			
	public void trainWordsFromText (JavaRDD<String> tokenizedText, Integer cr) throws IOException {
		Long count = tokenizedText.count();
		JavaPairRDD<Long, String> tokens = this.index(tokenizedText, count);
		tokens.foreach((entry) -> 
			this.trainWordFromContext(
					entry._2, 
					jsc.parallelize(
							Util.butCenter(
									Util.rangeNegToPos(cr),
									cr))
						.map((x) -> x + entry._1)
						.filter((x) -> 0 <= x && x < count)
						.map((x) -> tokens.lookup(x).get(0))));
	}

	public void trainWordsFromText (JavaRDD<String> tokenizedText) throws IOException {
		this.trainWordsFromText(tokenizedText, this.getCR());
	}
	*/
	
	public void trainWordsFromBatch (JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		int cr = this.getCR();
		Long count = tokenizedTexts.count();
		JavaPairRDD<Long, String> indexed = index(tokenizedTexts.values(), count);
		for (Long i = 0L; i < count; i++) {
			List<String> result = indexed.lookup(i);
			String res = result.get(0);
			String[] reses = res.split("\\s+");
			List<String> text = Arrays.asList(reses);
			this.trainWordsFromText(
					text,
					cr);
		}
	}
	
	public void clearDB (String name) {
		this.loadHBaseTable(name).keys().collect().forEach((x) -> {
			try {
				this.deleteRow(x);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
	
	
	//Files
	public JavaRDD<String> loadTextFile (String path) {
		FlatMapFunction<String, String> brk = 
				(str) -> Arrays.asList(str.split("\\s+"));
		return this.jsc.textFile(path).flatMap(brk);
	}
	
	public JavaPairRDD<String, String> loadTextDir (String path) {
		return this.jsc.wholeTextFiles(path);
	}
	
	//Static
	public static byte[] sToBytes (String s) { return HBase.stringToBytes(s); }
	public static String bToString (byte[] b) { return HBase.bytesToString(b); }	
	
	public static HadoopConf newConf () {
		return HBaseConf.create();
	}
	public static Configuration newConf (String tableName) {
		HadoopConf conf = newConf();
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		return conf;
	}
	
	public static Configuration rddConf (JavaPairRDD<?, ?> rdd) {
		return newConf(rdd.name());
	}
	public static Configuration rddConf (JavaRDD<?> rdd) {
		return newConf(rdd.name());
	}
}