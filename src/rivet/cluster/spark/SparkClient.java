package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
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
	
	public RIV trainWordFromContext (List<String> wordWithContext) throws IOException {
		List<String> context = new ArrayList<>(wordWithContext);
		String word = context.remove(0);
		return this.trainWordFromContext(word, context);
	}

	public void trainWordsFromText (List<String> tokenizedText, Integer cr) throws IOException {
		Integer count = tokenizedText.size();
		Long t = System.currentTimeMillis();
		Map<Long, String> tokens = Util.index(tokenizedText, count.longValue());
		tokens.forEach((index, value) -> {
			Util.printTimeEntry(t, index, count);
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
		System.out.println("Processing Files...");
		int cr = this.getCR();
		Long count = tokenizedTexts.count();
		JavaPairRDD<Long, String> indexed = Util.index(tokenizedTexts.values());
		for (Long i = 0L; i < count; i++) {
			System.out.println((i + 1) / count + "%: File# " + (i + 1));
			List<String> result = indexed.lookup(i);
			String res = result.get(0);
			String[] reses = res.split("\\s+");
			List<String> text = Arrays.asList(reses);
			System.out.println("Training: " + text.size() + " words.");
			this.trainWordsFromText(
					text,
					cr);
		}
	}
			
	public void trainWordsFromText (JavaRDD<List<String>> tokenizedText, Integer cr) throws IOException {
		tokenizedText.foreach(this::trainWordFromContext);
	}

	public void trainWordsFromText (JavaRDD<List<String>> tokenizedText) throws IOException {
		this.trainWordsFromText(tokenizedText, this.getCR());
	}
	
	private static List<String> getContext (String word, List<String> tokens, int cr) {
		int i = tokens.indexOf(word);
		List<String> context = Util.butCenter(Util.rangeNegToPos(cr), cr)
									.stream()
									.map((n) -> tokens.get(i + n))
									.filter((s) -> s != null)
									.collect(Collectors.toList());
		context.add(0, tokens.get(i));
		return context;
	}
	
	private static List<List<String>> breakAndGetContext (String text, int cr) {
		List<String> tokens = Arrays.asList(text.split("\\s+"));
		return tokens.stream()
			.map((word) -> getContext(word, tokens, cr))
			.collect(Collectors.toList());
	}
	
	public void trainWordsFromBatch (String path) {
		int cr = this.getCR();
		try (JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(1))) {
			JavaDStream<List<String>> stream = jssc.textFileStream(path).flatMap((text) -> breakAndGetContext(text, cr));
			stream.foreachRDD((tokens) -> this.trainWordsFromText(tokens, cr));
			jssc.start();
			jssc.awaitTermination();
		}
	}
	
	//Files
	public JavaRDD<List<String>> loadTextFile (String path) throws IOException {
		int cr = this.getCR();
		List<String> tokens = Files.lines(Paths.get(path))
								.flatMap((line) -> Arrays.stream(line.split("\\s+")))
								.collect(Collectors.toList());
		return this.jsc.parallelize(
				Util.mapList(
						(word) -> getContext(word, tokens, cr),
						tokens));		
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
	
	public static Configuration rddConf (JavaRDDLike<?, ?> rdd) {
		return newConf(rdd.name());
	}
	//public static Configuration rddConf (JavaRDD<?> rdd) {
	//	return newConf(rdd.name());
	//}
}