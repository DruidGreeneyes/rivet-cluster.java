package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import rivet.core.hashlabels.RIV;
import rivet.core.hashlabels.HashLabels;
import rivet.persistence.hbase.HBase;
import rivet.util.Util;
import scala.Tuple2;

import testing.Log;

import static java.util.Arrays.stream;
import static rivet.util.Util.setting;


public class SparkClient implements Closeable {	
	private static final Log log = new Log("test/sparkClientOutput.txt");
	
	private static final String LEX_COLUMN = "lex";
	
	private static final String META_ROW = "metadata";

	private static final String SIZE_COLUMN = "size";
	private static final String K_COLUMN = "k";
	private static final String CR_COLUMN = "cr";
	
	private static final Integer DEFAULT_SIZE = 16000;
	private static final Integer DEFAULT_K = 48;
	private static final Integer DEFAULT_CR = 3;
	
	private Integer size;
	private Integer k;
	private Integer cr;
	
	private String name;
	
	private final JavaSparkContext jsc;
	private JavaPairRDD<String, Row> rdd;
	
	private interface SFn<A, R> extends org.apache.spark.api.java.function.Function<A, R> {}
	private static <A, R> SFn<A, R> sfn (Function<A, R> jfn) {return jfn::apply;}
	
	//Class necessities
	@SafeVarargs
	public SparkClient (final String tableName, final String master, Tuple2<String, String>...settings) throws IOException {
		final SparkConf sparkConf = new SparkConf()
				.setAppName("Rivet")
				.setMaster(master);
		stream(settings).forEach((entry) -> sparkConf.set(entry._1, entry._2));
		this.jsc = new JavaSparkContext(sparkConf);
		this.rdd = this.jsc.newAPIHadoopRDD(
				HBase.newConf(tableName),
				TableInputFormat.class,
				ImmutableBytesWritable.class, 
				Result.class)
			.mapToPair(Spark::prepareEntryForWork);
		
		this.name = tableName;
			
		log.log(this.name);
		this.size = this.getMetadata(SIZE_COLUMN).orElse(DEFAULT_SIZE);
		this.k = this.getMetadata(K_COLUMN).orElse(DEFAULT_K);
		this.cr = this.getMetadata(CR_COLUMN).orElse(DEFAULT_CR);
	}
		
	public SparkClient (final String tableName) throws IOException {
		this(tableName, "local[3]");
	}
	
	@Override
	public void close() throws IOException {this.jsc.close();}
	
	
	//low-level	
	public SparkClient clear() {
		this.rdd = this.rdd.subtractByKey(this.rdd);
		return this;
	}
	
	public long count() {
		return this.rdd.count();
	}
	
	public SparkClient write() throws IOException {
		Configuration conf = HBase.newConf(
				setting(TableOutputFormat.OUTPUT_TABLE, this.name),
				setting("hbase.zookeeper.quorum", "localhost"));
		Job job = Job.getInstance(conf);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		this.rdd
			.mapToPair(Spark::prepareEntryForStorage)
			.saveAsNewAPIHadoopDataset(job.getConfiguration());
		return this;
	}
	
	public Integer getSize () {return this.size;}
	public Integer getK () {return this.k;}
	public Integer getCR () {return this.cr;}
	
	public SparkClient setSize (final Integer size) {this.size = size; return this;}
	public SparkClient setK (final Integer k) {this.k = k; return this;}
	public SparkClient setCR (final Integer cr) {this.cr = cr; return this;}
	
	public static Function<Tuple2<String, Result>, Boolean> 
		matchKey (String key) {
		return (entry) -> entry._1.compareTo(key) == 0;
	}
	public static Function<Tuple2<String, Result>, Boolean> 
		notMatchKey (String key) {
		return (entry) -> entry._1.compareTo(key) != 0;
	}
	
	public Optional<Row> get (String key) {
		return Util.getOpt(this.rdd.lookup(key), 0);
	}
	
	
	public static Function<Row, String> pointGetter(String columnKey) {
		return (row) -> row.get(columnKey);
	}
	
	public static String getPoint (Row row, String columnKey) {
		return pointGetter(columnKey).apply(row);
	}
	public Optional<String> getPoint (String key, String columnKey) {
		return this.get(key).map(pointGetter(columnKey));
	}
	
	public Optional<Integer> getMetadata (String item) {
		return this.getPoint(META_ROW, item).map(Integer::parseInt);
	}
	
	//Mid-Level		
	public static RIV getWordInd (final Integer size, final Integer k, final String word) {
		return HashLabels.generateLabel(size, k, word);
	}
	
	public RIV getWordInd (final String word) {
		return getWordInd(this.size, this.k, word);
	}
	
	public Optional<RIV> getWordLex (final String word) {
		return this.get(word)
				.map(pointGetter(LEX_COLUMN))
				.map(RIV::new);
	}
	
	public RIV getOrMakeWordLex (final String word) {
		return this.getWordLex(word)
				.orElse(this.getWordInd(word));
	}
	
	public RIV getMeanVector() {
		final Long count = this.rdd.count();
		return this.rdd
				.values()
				.map(sfn(pointGetter(LEX_COLUMN)))
				.map(RIV::new)
				.map(RIV::normalize)
				.reduce(HashLabels::addLabels)
				.divideBy(count);
	}
	
	//Training
	public SparkClient batchTrainer (
			final JavaPairRDD<String, String> texts, 
			final PairFlatMapFunction<Tuple2<String, String>, String, RIV> trainer) {
		log.log("Processing Files: %d files", texts.count());
		final JavaPairRDD<String, RIV> lexes = 
				texts.flatMapToPair(trainer)
					.reduceByKey(HashLabels::addLabels);
		this.rdd = this.rdd.fullOuterJoin(lexes)
						.mapValues((v) -> Tuple2.apply(
								Util.gOptToJOpt(v._1),
								Util.gOptToJOpt(v._2)))
						.mapValues((v) -> Spark.mergeJoinEntry(v));
		return this;
	}
	
	private static Tuple2<String, RIV> getContextRIV (final List<String> tokens, final Integer index, final Integer size, final Integer k, final Integer cr) {
		final Integer count = tokens.size();
		return Tuple2.apply(
				tokens.get(index), 
				Util.butCenter(Util.rangeNegToPos(cr), cr)
					.filter(n -> 0 <= n && n < count)
					.mapToObj(n -> tokens.get(n))
					.map(word -> getWordInd(size, k, word))
					.reduce(new RIV(size, k), HashLabels::addLabels));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetContextRIVs (final Tuple2<String, String> textEntry, final Integer size, final Integer k, final Integer cr) throws IOException {
		final String text = textEntry._2;
		final List<String> words = Arrays.asList(text.split("\\s+"));
		final Integer count = words.size();
		log.log("Processing file: %s    ----    %d words.", textEntry._1, count);
		return Util.range(count)
				.mapToObj((index) -> {
					return getContextRIV(words, index, size, k, cr);
				})
				.collect(Collectors.toList());
	}
	
	public SparkClient trainWordsFromBatch (final JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		final Integer size = this.size;
		final Integer k = this.k;
		final Integer cr = this.cr;
		return this.batchTrainer(
				tokenizedTexts,
				(textEntry) -> breakAndGetContextRIVs(textEntry, size, k, cr));
	}
	
	public SparkClient trainWordsFromBatch (final String path) throws IOException {
		return this.trainWordsFromBatch(this.loadTextDir(path));
	}
	
	@SuppressWarnings("unused")
	private static Stream<Tuple2<String, RIV>> getSentenceRIVs (final String[] sentence, final Integer size, final Integer k) {
		final RIV sum = Arrays.stream(sentence)
					.map((word) -> getWordInd(size, k, word))
					.reduce(new RIV(size, k), HashLabels::addLabels);
		return Arrays.stream(sentence)
				.map((word) -> Tuple2.apply(
						word,
						sum.subtract(getWordInd(size, k, word))));
	}
	
	private static Tuple2<String, RIV> getPermutedContextRIV (final String[] context, final Integer index, final Integer size, final Integer k, Tuple2<int[], int[]> permutations) {
		final Integer count = context.length;
		log.log("Processing sentence: %d words...", count);
		final RIV riv = Util.range(count)
					.filter((i) -> !index.equals(i))
					.mapToObj((i) -> 
						getWordInd(size, k, context[i])
							.permute(permutations, i - index))
					.reduce(new RIV(size, k), HashLabels::addLabels);
		return Tuple2.apply(context[index], riv);
	}
	
	private static Stream<Tuple2<String, RIV>> getPermutedSentenceRIVs (final String[] sentence, final Integer size, final Integer k, Tuple2<int[], int[]> permutations) {
		return Util.range(sentence.length)
					.mapToObj((index) -> 
						getPermutedContextRIV(sentence, index, size, k, permutations));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetSentenceRIVs (final Tuple2<String, String> textEntry, final Integer size, final Integer k) throws IOException {
		final String text = textEntry._2;
		final List<String> sentences = Arrays.asList(text.split("\\n+"));
		final Integer count = sentences.size();
		final Tuple2<int[], int[]> permutations = HashLabels.generatePermutations(size);
		log.log("Processing file: %s    ----    %d sentences.", textEntry._1, count);
		return sentences.parallelStream()
					.flatMap((sentence) -> 
						getPermutedSentenceRIVs(sentence.split("\\s+"), size, k, permutations))
					.collect(Collectors.toList());
	}
	
	public SparkClient trainWordsFromSentenceBatch (final JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		final Integer size = this.getSize();
		final Integer k = this.getK();
		return this.batchTrainer(
				tokenizedTexts,
				(textEntry) -> 
					breakAndGetSentenceRIVs(textEntry, size, k));
	}
	
	public SparkClient trainWordsFromSentenceBatch (final String path) throws IOException {
		return this.trainWordsFromSentenceBatch(this.loadTextDir(path));
	}
	
	//Streaming
	
	
	
	//Documents
	public RIV lexDocument (final String document) {
		return this.jsc.parallelize(Arrays.asList(document.split("\\s+")))
				.mapToPair((s) -> Tuple2.apply(s, Optional.empty()))
				.leftOuterJoin(this.rdd)
				.map(Tuple2::_2)
				.map(Tuple2::_2)
				.map(Util::gOptToJOpt)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.map(sfn(Row.getter("lex")))
				.map(RIV::new)
				.reduce(HashLabels::addLabels)
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