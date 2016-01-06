package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import rivet.Util;
import rivet.cluster.Spark;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.persistence.HBase;

import scala.Tuple2;


public class SparkClient implements Closeable {	
	private final int size = 16000;
	private final int k = 48;
	private final int cr = 3;
	
	private final PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Row> B2BA = 
			(x) -> {
				Row cells = new Row();
				x._2.getFamilyMap(HBase.stringToBytes(DATA)).forEach((k, v) -> cells.put(HBase.bytesToString(k),
																	HBase.bytesToString(v)));
				return Tuple2.apply(Spark.ibwToString(x._1), cells);
			};
	
	public JavaSparkContext jsc;
	public SparkTable sparkTable;
	
	public static String DATA = "data";
	public static String LEX = "lex";
	
	public static String META = "metadata";

	public static String SIZE = "size";
	public static String K = "k";
	public static String CR = "cr";
	
	
	//Class necessities
	public SparkClient (String tableName, String master, String hostRam, String workerRam) throws IOException {
		SparkConf sparkConf = new SparkConf()
				.setAppName("Rivet")
				.setMaster(master)
				.set("spark.driver.memory", hostRam)
				.set("spark.executor.memory", workerRam);
		this.jsc = new JavaSparkContext(sparkConf);
		this.sparkTable = this.createSparkTable(tableName, "word");
	}
		
	public SparkClient (String tableName) throws IOException {
		this(tableName, "local[3]", "4g", "3g");
	}
	
	@Override
	public void close() throws IOException {
		this.sparkTable.close();
		this.jsc.close();
	}
	
	
	//low-level
	public SparkTable createSparkTable (String tableName, String rowsKey) throws IOException {
		Configuration conf = newConf(tableName);
		SparkTable ret = new SparkTable(
				rowsKey,
				this.jsc.newAPIHadoopRDD(
								conf,
								TableInputFormat.class,
								ImmutableBytesWritable.class, 
								Result.class)
							.mapToPair(B2BA)
							.setName(tableName));
		return ret;
	}
	
	public boolean clearTable () throws IOException {
		return this.sparkTable.clear();
	}
	
	public int getSize () {	
		return this.sparkTable.getMetadata(SIZE).orElse(this.size);
	}
	public int getK () { 
		return this.sparkTable.getMetadata(K).orElse(this.k);
	}
	public int getCR () { 
		return this.sparkTable.getMetadata(CR).orElse(this.cr); 
	}
	public Optional<Integer> setSize (int size) throws IOException {
		return this.sparkTable.setMetadata(SIZE, size);
	}
	public Optional<Integer> setK (int k) throws IOException {
		return this.sparkTable.setMetadata(K, k);
	}
	public Optional<Integer> setCR (int cr) throws IOException {
		return this.sparkTable.setMetadata(CR, cr);
	}
		
	public RIV getWordInd (String word) {
		System.out.println("getWordInd called with arg: " + word); 
		 return HashLabels.generateLabel(
				 this.getSize(),
				 this.getK(),
				 word);
	}
	public Optional<RIV> getWordLex (String word) {
		return this.sparkTable.getPoint(word, LEX).map(
				(x) -> RIV.fromString(x));
	}
	
	public RIV getOrMakeWordLex (String word) {
		return this.getWordLex(word)
				.orElse(this.getWordInd(word));
	}
	
	public Optional<RIV> setWordLex (String word, RIV lex) throws IOException {
		return this.sparkTable.setPoint(word, "lex", lex.toString())
				.map(RIV::fromString);
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
		return this.setWordLex(word, lex)
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
	
	public RIV trainWordFromContext (String word, JavaRDD<String> context) throws IOException {
		RIV lex = context.map(this::getWordInd)
					.fold(
						this.getOrMakeWordLex(word),
						HashLabels::addLabels);
		return this.setWordLex(word, lex)
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
	
	public void trainWordsFromBatch (String path) {
		int cr = this.getCR();
		FlatMapFunction<String, String> brk = 
				(str) -> Arrays.asList(str.split("\\s+"));
		VoidFunction<JavaRDD<String>> fun =
				(text) -> this.trainWordsFromText(text.flatMap(brk), cr);
		try (JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(1))) {
			JavaDStream<String> stream = jssc.textFileStream(path);
			stream.foreachRDD(fun);
			jssc.start();
			jssc.awaitTermination();
		}
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
	
	public static Configuration newConf (String tableName) {
		Configuration conf = HBaseConfiguration.create();
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