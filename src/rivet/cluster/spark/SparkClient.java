package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import rivet.util.Counter;
import scala.Tuple2;
import testing.Log;


public class SparkClient implements Closeable {	
	private static final Log log = new Log("test/sparkClientOutput.txt");;
	
	private final Integer size = 16000;
	private final Integer k = 48;
	private final Integer cr = 3;
	
	private final PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Row> B2BA = 
			(x) -> {
				Row cells = new Row();
				x._2.getFamilyMap(HBase.stringToBytes(DATA)).forEach((k, v) -> cells.put(HBase.bytesToString(k),
																	HBase.bytesToString(v)));
				return Tuple2.apply(Spark.ibwToString(x._1), cells);
			};
	
	private JavaSparkContext jsc;
	private SparkTable sparkTable;
	
	public static final String DATA = "data";
	public static final String LEX = "lex";
	
	public static final String META = "metadata";

	public static final String SIZE = "size";
	public static final String K = "k";
	public static final String CR = "cr";
	
	
	//Class necessities
	public SparkClient (final String tableName, final String master, final String hostRam, final String workerRam) throws IOException {
		final SparkConf sparkConf = new SparkConf()
				.setAppName("Rivet")
				.setMaster(master)
				.set("spark.driver.memory", hostRam)
				.set("spark.executor.memory", workerRam);
		this.jsc = new JavaSparkContext(sparkConf);
		this.sparkTable = this.createSparkTable(tableName, "word");
	}
		
	public SparkClient (final String tableName) throws IOException {
		this(tableName, "local[3]", "4g", "3g");
	}
	
	@Override
	public void close() throws IOException {
		this.sparkTable.close();
		this.jsc.close();
	}
	
	
	//low-level
	public SparkTable createSparkTable (final String tableName, final String rowsKey) throws IOException {
		final Configuration conf = Spark.newConf(tableName);
		return new SparkTable(
				rowsKey,
				this.jsc.newAPIHadoopRDD(
								conf,
								TableInputFormat.class,
								ImmutableBytesWritable.class, 
								Result.class)
							.mapToPair(B2BA)
							.setName(tableName));
	}
	
	public boolean clearTable () throws IOException {
		return this.sparkTable.clear();
	}
	
	public Integer getSize () {	
		return this.sparkTable.getMetadata(SIZE).orElse(this.size);
	}
	public Integer getK () { 
		return this.sparkTable.getMetadata(K).orElse(this.k);
	}
	public Integer getCR () { 
		return this.sparkTable.getMetadata(CR).orElse(this.cr); 
	}
	public Optional<Integer> setSize (final Integer size) throws IOException {
		return this.sparkTable.setMetadata(SIZE, size);
	}
	public Optional<Integer> setK (final Integer k) throws IOException {
		return this.sparkTable.setMetadata(K, k);
	}
	public Optional<Integer> setCR (final Integer cr) throws IOException {
		return this.sparkTable.setMetadata(CR, cr);
	}
		
	public static RIV getWordInd (final Integer size, final Integer k, final String word) {
		return HashLabels.generateLabel(size, k, word);
	}
	
	public RIV getWordInd (final String word) {
		return getWordInd(this.getSize(), this.getK(), word);
	}
	
	public Optional<RIV> getWordLex (final String word) {
		return this.sparkTable.getPoint(word, LEX).map(
				(x) -> new RIV(x));
	}
	
	public RIV getOrMakeWordLex (final String word) {
		return this.getWordLex(word)
				.orElse(this.getWordInd(word));
	}
	
	public Optional<RIV> setWordLex (final String word, final RIV lex) throws IOException {
		return this.sparkTable.setPoint(word, "lex", lex.toString())
				.map(RIV::new);
	}
	
	public RIV getMeanVector() {
		final Long count = this.sparkTable.count();
		return this.sparkTable.rdd
						.values()
						.map((row) -> new RIV(row.get(LEX)).normalize())
						.reduce(HashLabels::addLabels)
						.divideBy(count);
	}
		
	private static Tuple2<String, RIV> getContextRIV (final List<String> tokens, final Integer index, final Integer size, final Integer k, final Integer cr) {
		final Integer count = tokens.size();
		return Tuple2.apply(
				tokens.get(index), 
				Util.butCenter(Util.rangeNegToPos(cr), cr)
					.parallel()
					.filter((n -> 0 <= n && n < count))
					.mapToObj((n) -> tokens.get(n))
					.map((word) -> getWordInd(size, k, word))
					.reduce(new RIV(size, k), HashLabels::addLabels));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetContextRIVs (final Tuple2<String, String> textEntry, final Integer size, final Integer k, final Integer cr) throws IOException {
		final String text = textEntry._2;
		final List<String> words = Arrays.asList(text.split("\\s+"));
		final Integer count = words.size();
		log.log("Processing file: %s    ----    %d words.", textEntry._1, count);
		final Instant t = Instant.now();
		final Counter c = new Counter();
		return Util.range(count)
				.parallel()
				.mapToObj((index) -> {
					log.logTimeEntry(t, c.inc(), count);
					return getContextRIV(words, index, size, k, cr);
				})
				.collect(Collectors.toList());
	}
	
	public void trainWordsFromBatch (final JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		final Instant startTime = Instant.now();
		final Integer size = this.getSize();
		final Integer k = this.getK();
		final Integer cr = this.getCR();
		log.log("Processing Files: %d files", tokenizedTexts.count());
		final List<Tuple2<String, RIV>> tuples = 
				tokenizedTexts.flatMapToPair(
						(textEntry) -> breakAndGetContextRIVs(textEntry, size, k, cr))
					.reduceByKey(HashLabels::addLabels)
					.collect();
		final Instant t = Instant.now();
		final Counter c = new Counter();
		final Long count = (long)tuples.size();
		log.log("Updating %d word entries...", tuples.size());
		tuples.parallelStream().forEach((entry) -> {
			log.logTimeEntry(t, c.inc(), count);
			final RIV lex = HashLabels.addLabels(
					entry._2, 
					this.getOrMakeWordLex(entry._1));
			try {
				this.setWordLex(entry._1, lex);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		log.log("Complete: %s elapsed.", Util.timeSince(startTime));
	}
	
	public void trainWordsFromBatch (final String path) throws IOException {
		this.trainWordsFromBatch(this.loadTextDir(path));
	}
	
	@SuppressWarnings("unused")
	private static Stream<Tuple2<String, RIV>> getSentenceRIVs (final String[] sentence, final Integer size, final Integer k) {
		final RIV sum = Arrays.stream(sentence)
					.parallel()
					.map((word) -> getWordInd(size, k, word))
					.reduce(new RIV(size, k), HashLabels::addLabels);
		return Arrays.stream(sentence)
				.map((word) -> Tuple2.apply(
						word,
						sum.subtract(getWordInd(size, k, word))));
	}
	
	private static Tuple2<String, RIV> getPermutedContextRIV (final String[] context, final Integer index, final Integer size, final Integer k) {
		final RIV riv = Util.range(context.length)
					.parallel()
					.filter((i) -> !index.equals(i))
					.mapToObj((i) -> 
						getWordInd(size, k, context[i])
							.permuteFromZero((long)i - index))
					.reduce(new RIV(size, k), HashLabels::addLabels);
		return Tuple2.apply(context[index], riv);
	}
	
	private static Stream<Tuple2<String, RIV>> getPermutedSentenceRIVs (final String[] sentence, final Integer size, final Integer k) {
		return Util.range(sentence.length)
					.parallel()
					.mapToObj((index) -> 
						getPermutedContextRIV(sentence, index, size, k));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetSentenceRIVs (final Tuple2<String, String> textEntry, final Integer size, final Integer k) throws IOException {
		final String text = textEntry._2;
		final List<String> sentences = Arrays.asList(text.split("\\n+"));
		final Integer count = sentences.size();
		log.log("Processing file: %s    ----    %d sentences.", textEntry._1, count);
		final Instant t = Instant.now();
		final Counter c = new Counter();
		return sentences.parallelStream()
					.flatMap((sentence) -> {
						log.logTimeEntry(t, c.inc(), count);
						return getPermutedSentenceRIVs(sentence.split("\\s+"), size, k);
					})
					.collect(Collectors.toList());
	}
	
	public void trainWordsFromSentenceBatch (final JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		final Instant startTime = Instant.now();
		final Integer size = this.getSize();
		final Integer k = this.getK();
		log.log("Processing Files: %d files", tokenizedTexts.count());
		final List<Tuple2<String, RIV>> tuples = 
				tokenizedTexts.flatMapToPair(
						(textEntry) -> breakAndGetSentenceRIVs(textEntry, size, k))
				.reduceByKey(HashLabels::addLabels)
				.collect();
		final Instant t = Instant.now();
		final Counter c = new Counter();
		final Long count = (long)tuples.size();
		log.log("Updating %d word entries...", tuples.size());
		tuples.parallelStream().forEach((entry) -> {
			log.logTimeEntry(t, c.inc(), count);
			final RIV lex = HashLabels.addLabels(
					entry._2, 
					this.getOrMakeWordLex(entry._1));
			try {
				this.setWordLex(entry._1, lex);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		log.log("Complete: %s elapsed.", Util.timeSince(startTime));
	}
	
	public void trainWordsFromSentenceBatch (final String path) throws IOException {
		this.trainWordsFromSentenceBatch(this.loadTextDir(path));
	}
	
	public RIV lexDocument (final String document) {
		return Arrays.stream(document.split("\\s+"))
				.parallel()
				.map((word) -> this.getOrMakeWordLex(word).normalize())
				.reduce(new RIV(this.getSize(), this.getK()), HashLabels::addLabels)
				.subtract(this.getMeanVector());
	}
	
	//Files
	public static String loadTextFile (final String path) throws IOException {
		return String.join(
				" ",
				Files.readAllLines(Paths.get(path)));
	}
	
	public JavaPairRDD<String, String> loadTextDir (final String path) {
		return this.jsc.wholeTextFiles(path);
	}
}