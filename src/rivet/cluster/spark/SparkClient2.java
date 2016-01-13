package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import rivet.Util;
import rivet.cluster.Spark;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.persistence.HBase;


import scala.Tuple2;
import testing.Log;

public class SparkClient2 implements Closeable { 
	private static Log log;
	
	public SparkTable sparkTable;
	public JavaSparkContext jsc;
	
	private static final int DEFAULT_SIZE = 16000;
	private static final int DEFAULT_K = 48;
	private static final int DEFAULT_CR = 3;
	
	private static final String DATA = "data";
	private static final String LEX = "lex";

	private static final String SIZE = "size";
	private static final String K = "k";
	private static final String CR = "cr";
	
	private static Tuple2<String, Row> convertRDDElementToSerializedFormats (Tuple2<ImmutableBytesWritable, Result> entry) {
		Row cells = new Row();
		entry._2.getFamilyMap(HBase.stringToBytes(DATA))
				.forEach((k, v) -> cells.put(HBase.bytesToString(k),
											 HBase.bytesToString(v)));
		return Tuple2.apply(Spark.ibwToString(entry._1), cells);
	}
	
	public SparkClient2(String tableName, String master, String hostRam, String workerRam) throws IOException {
		SparkConf sparkConf = new SparkConf()
				.setAppName("Rivet")
				.setMaster(master)
				.set("spark.driver.memory", hostRam)
				.set("spark.executor.memory", workerRam);
		this.jsc = new JavaSparkContext(sparkConf);
		this.sparkTable = this.createSparkTable(tableName, "word");
		log = new Log("data/sparkClient2Output.txt");
	}
	
	@Override
	public void close() throws IOException {
		this.sparkTable.close();
		this.jsc.close();
		log.close();
	}
	
	public SparkTable createSparkTable (String tableName, String rowsKey) throws IOException {
		Configuration conf = Spark.newConf(tableName);
		SparkTable ret = new SparkTable(
				rowsKey,
				this.jsc.newAPIHadoopRDD(
								conf,
								TableInputFormat.class,
								ImmutableBytesWritable.class, 
								Result.class)
							.mapToPair(SparkClient2::convertRDDElementToSerializedFormats)
							.setName(tableName));
		return ret;
	}
	
	public boolean clearTable () throws IOException {
		return this.sparkTable.clear();
	}
		
	public int getSize () {	
		return this.sparkTable.getMetadata(SIZE).orElse(DEFAULT_SIZE);
	}
	public int getK () { 
		return this.sparkTable.getMetadata(K).orElse(DEFAULT_K);
	}
	public int getCR () { 
		return this.sparkTable.getMetadata(CR).orElse(DEFAULT_CR); 
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
	
	public static RIV getWordInd (int size, int k, String word) {
		return HashLabels.generateLabel(size, k, word);
	}
	
	public RIV getWordInd (String word) {
		return getWordInd(this.getSize(), this.getK(), word);
	}
	
	public Optional<RIV> getWordLex (String word) {
		return this.sparkTable.getPoint(word, LEX).map(RIV::new);
	}
	
	public RIV getOrMakeWordLex (String word) {
		return this.getWordLex(word)
				.orElse(this.getWordInd(word));
	}
	
	public Optional<RIV> setWordLex (String word, RIV lex) throws IOException {
		return this.sparkTable.setPoint(word, "lex", lex.toString())
				.map(RIV::new);
	}
	
	private static Tuple2<String, RIV> getContextRIV (List<String> tokens, int index, int size, int k, int cr) {
		return Tuple2.apply(
				tokens.get(index), 
				Util.butCenter(Util.rangeNegToPos(cr), cr)
					.parallel()
					.mapToObj((n) -> tokens.get(index + n))
					.filter((word) -> word != null)
					.map((word) -> getWordInd(size, k, word))
					.reduce(new RIV(), HashLabels::addLabels));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetContextRIVs (String text, int size, int k, int cr) throws IOException {
		List<String> words = Arrays.asList(text.split("\\s+"));
		int count = words.size();
		Instant t = Instant.now();
		log.log("Processing line, step 1: " + count + " words.");
		return Util.range(count)
				.parallel()
				.mapToObj((index) -> {
					log.logTimeEntry(t, index, count);
					return getContextRIV(words, index, size, k, cr);
				})
				.collect(Collectors.toList());
	}
	
	public void trainWordsFromBatch (String path) throws IOException {
		int size = this.getSize();
		int k = this.getK();
		int cr = this.getCR();
		try (JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(1))) {
			JavaPairDStream<String, RIV> stream = 
					jssc.textFileStream(path) 
						.flatMapToPair((text) -> breakAndGetContextRIVs(text, size, k, cr));
			stream.foreachRDD((rdd) -> {
				Map<Tuple2<String, RIV>, Long> map = rdd.zipWithIndex().collectAsMap();
				int count = map.size();
				Instant t = Instant.now();
				System.out.println("Processing file, step 2");
				map.forEach((entry, index) -> {
					log.logTimeEntry(t, index, count);
					try {
						this.setWordLex(
								entry._1,
								HashLabels.addLabels(
										this.getOrMakeWordLex(entry._1),
										entry._2));
					} catch (IOException e) {
						e.printStackTrace();
					}
				});
			});
			jssc.start();
			jssc.awaitTermination();
		}
	}
	
	//Files
	
	
	
	public JavaRDD<String> loadTextFile (String path) throws IOException {
		return this.jsc.textFile(path);
	}
	
	public Queue<JavaRDD<String>> loadTextDir (String path) throws IOException {
		Stream<Path> s = Files.list(Paths.get(path)).filter((p) -> !Files.isDirectory(p));
		Queue<JavaRDD<String>> q = new ConcurrentLinkedDeque<>();
		s.forEach((p) -> q.add(this.jsc.textFile(p.toUri().getPath())));
		return q;
	}
}