package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import rivet.Util;
import rivet.cluster.Spark;
import rivet.core.HashLabels;
import rivet.core.RIV;
import rivet.persistence.HBase;

import scala.Tuple2;

import testing.Log;


public class SparkClient implements Closeable {	
	private static Log log;
	
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
	
	private JavaSparkContext jsc;
	private SparkTable sparkTable;
	
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
		log = new Log("test/sparkClientOutput.txt");
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
		Configuration conf = Spark.newConf(tableName);
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
		
	public static RIV getWordInd (int size, int k, String word) {
		return HashLabels.generateLabel(size, k, word);
	}
	
	public RIV getWordInd (String word) {
		return getWordInd(this.getSize(), this.getK(), word);
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
		
	private static Tuple2<String, RIV> getContextRIV (List<String> tokens, int index, int size, int k, int cr) {
		int count = tokens.size();
		return Tuple2.apply(
				tokens.get(index), 
				Util.butCenter(Util.rangeNegToPos(cr), cr)
					.stream()
					.filter((n -> 0 <= n && n < count))
					.map((n) -> tokens.get(n))
					.map((word) -> getWordInd(size, k, word))
					.reduce(new RIV(), HashLabels::addLabels));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetContextRIVs (Tuple2<String, String> textEntry, int size, int k, int cr) throws IOException {
		String text = textEntry._2;
		List<String> words = Arrays.asList(text.split("\\s+"));
		int count = words.size();
		Instant t = Instant.now();
		log.log("Processing file: %s    ----    %d words.", textEntry._1, count);
		return Util.mapList(
				(index) -> {
					log.logTimeEntry(t, index, count);
					return getContextRIV(words, index, size, k, cr);
				},
				Util.range(count));
	}
	
	public void trainWordsFromBatch (JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		int size = this.getSize();
		int k = this.getK();
		int cr = this.getCR();
		log.log("Processing Files: %d files", tokenizedTexts.count());
		List<Tuple2<String, RIV>> tuples = 
				tokenizedTexts.flatMapToPair(
						(textEntry) -> breakAndGetContextRIVs(textEntry, size, k, cr))
					.reduceByKey(HashLabels::addLabels)
					.collect();
		log.log("Updating word entries...");
		long count = tuples.size();
		Instant t = Instant.now();
		tuples.stream().forEach((entry) -> {
			int index = tuples.indexOf(entry);
			log.logTimeEntry(t, index, count);
			RIV lex = HashLabels.addLabels(
					entry._2, 
					this.getOrMakeWordLex(entry._1));
			try {
				this.setWordLex(entry._1, lex);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		log.log("Complete: %s elapsed.", Util.parseTimeString(Duration.between(t, Instant.now()).toString()));
	}
	
	public void trainWordsFromBatch (String path) throws IOException {
		this.trainWordsFromBatch(this.loadTextDir(path));
	}
	
	//Files
	public String loadTextFile (String path) throws IOException {
		return String.join(
				" ",
				Files.readAllLines(Paths.get(path)));
	}
	
	public JavaPairRDD<String, String> loadTextDir (String path) {
		return this.jsc.wholeTextFiles(path);
	}
}