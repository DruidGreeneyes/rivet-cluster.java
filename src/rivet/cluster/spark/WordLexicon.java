package rivet.cluster.spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import rivet.core.arraylabels.RIV;
import rivet.core.arraylabels.Labels;
import rivet.util.Util;
import scala.Tuple2;
import testing.Log;


public class WordLexicon extends Lexicon {
	private Log log = new Log("test/wordLexiconOutput");

	private Integer cr;
	
	//Class necessities
	public WordLexicon (final JavaSparkContext jsc, final String hbaseTableName) throws IOException {
		super(jsc, hbaseTableName);
		this.cr = this.getMetadata(CR_COLUMN).orElse(DEFAULT_CR);
	}
	
	//Training
	public WordLexicon trainer (
			final JavaRDD<String> text,
			final PairFlatMapFunction<String, String, RIV> trainer) {
		final JavaPairRDD<String, RIV> lexes = 
				text.flatMapToPair(trainer)
					.reduceByKey(Labels::addLabels);
		this.rdd = this.rdd.fullOuterJoin(lexes)
				.mapValues((v) -> new Tuple2<>(
						Util.gOptToJOpt(v._1),
						Util.gOptToJOpt(v._2)))
				.mapValues((v) -> Spark.mergeJoinEntry(v));
		return this;
	}
	
	public WordLexicon batchTrainer (
			final JavaPairRDD<String, String> texts, 
			final PairFlatMapFunction<Tuple2<String, String>, String, RIV> trainer) {
		final JavaPairRDD<String, RIV> lexes = 
				texts.flatMapToPair(trainer)
					.reduceByKey(Labels::addLabels);
		this.rdd = this.rdd.fullOuterJoin(lexes)
						.mapValues((v) -> new Tuple2<>(
								Util.gOptToJOpt(v._1),
								Util.gOptToJOpt(v._2)))
						.mapValues((v) -> Spark.mergeJoinEntry(v));
		return this;
	}
	
	private static Tuple2<String, RIV> getContextRIV (final List<String> tokens, final Integer index, final Integer size, final Integer k, final Integer cr) {
		final Integer count = tokens.size();
		return new Tuple2<>(
				tokens.get(index), 
				Util.butCenter(Util.rangeNegToPos(cr), cr)
					.filter(n -> 0 <= n && n < count)
					.mapToObj(n -> tokens.get(n))
					.map(word -> getInd(size, k, word))
					.reduce(new RIV(size), Labels::addLabels));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetContextRIVs (final Tuple2<String, String> textEntry, final Integer size, final Integer k, final Integer cr) throws IOException {
		final String text = textEntry._2;
		final List<String> words = Arrays.asList(text.split("\\s+"));
		final Integer count = words.size();
		return Util.range(count)
				.mapToObj((index) -> {
					return getContextRIV(words, index, size, k, cr);
				})
				.collect(Collectors.toList());
	}
	
	public WordLexicon trainWordsFromBatch (final JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		final Integer size = this.size;
		final Integer k = this.k;
		final Integer cr = this.cr;
		return this.batchTrainer(
				tokenizedTexts,
				(textEntry) -> breakAndGetContextRIVs(textEntry, size, k, cr));
	}
	
	@SuppressWarnings("unused")
	private static Stream<Tuple2<String, RIV>> getSentenceRIVs (final String[] sentence, final Integer size, final Integer k) {
		final RIV sum = Arrays.stream(sentence)
					.map((word) -> getInd(size, k, word))
					.reduce(new RIV(size), Labels::addLabels);
		return Arrays.stream(sentence)
				.map((word) -> new Tuple2<>(
						word,
						sum.subtract(getInd(size, k, word))));
	}
	
	private static Tuple2<String, RIV> getPermutedContextRIV (final String[] context, final Integer index, final Integer size, final Integer k, Tuple2<int[], int[]> permutations) {
		final Integer count = context.length;
		final RIV riv = Util.range(count)
					.filter((i) -> !index.equals(i))
					.mapToObj((i) -> 
						getInd(size, k, context[i])
							.permute(permutations, i - index))
					.reduce(new RIV(size), Labels::addLabels);
		return new Tuple2<>(context[index], riv);
	}
	
	private static Stream<Tuple2<String, RIV>> getPermutedSentenceRIVs (final String[] sentence, final Integer size, final Integer k, Tuple2<int[], int[]> permutations) {
		return Util.range(sentence.length)
					.mapToObj((index) -> 
						getPermutedContextRIV(sentence, index, size, k, permutations));
	}
	
	private static List<Tuple2<String, RIV>> breakAndGetSentenceRIVs (final Tuple2<String, String> textEntry, final Integer size, final Integer k) throws IOException {
		final String text = textEntry._2;
		final List<String> sentences = Arrays.asList(text.split("\\n+"));
		final Tuple2<int[], int[]> permutations = Labels.generatePermutations(size);
		return sentences.parallelStream()
					.flatMap((sentence) -> 
						getPermutedSentenceRIVs(sentence.split("\\s+"), size, k, permutations))
					.collect(Collectors.toList());
	}
	
	public WordLexicon trainWordsFromSentenceBatch (final JavaPairRDD<String, String> tokenizedTexts) throws IOException {
		final Integer size = this.getSize();
		final Integer k = this.getK();
		return this.batchTrainer(
				tokenizedTexts,
				(textEntry) ->
					breakAndGetSentenceRIVs(textEntry, size, k));
	}
	
	public WordLexicon trainWordsFromSentenceFile (final JavaRDD<String> text) throws IOException {
		final Integer size = this.getSize();
		final Integer k = this.getK();
		Tuple2<int[], int[]> permutations = Labels.generatePermutations(size);
		return this.trainer(text,
				(line) ->
					getPermutedSentenceRIVs(line.split("\\s+"), size, k, permutations)
					.collect(Collectors.toList()));
	}
	
	public String uiTrain(String path) {
		File file = new File(path);
		if (file.isDirectory()){
			log.log("attempting to load filed in directory: %s", file.getAbsolutePath());
			JavaPairRDD<String, String> texts = jsc.wholeTextFiles("file://" + file.getAbsolutePath());
			long fileCount = texts.count();
			long startCount = this.count();
			try {
				this.trainWordsFromSentenceBatch(texts);
			} catch (IOException e) {
				e.printStackTrace();
				return e.getMessage();
			}
			long wordsAdded = this.count() - startCount;
			return String.format("Batch training complete. %d files processed, %d words added to lexicon.",
									fileCount, wordsAdded);
		} else {
			JavaRDD<String> text = jsc.textFile(path);
			long lineCount = text.count();
			long startCount = this.count();
			try {
				this.trainWordsFromSentenceFile(text);
			} catch (IOException e) {
				e.printStackTrace();
				return e.getMessage();
			}
			long wordsAdded = this.count() - startCount;
			return String.format("Batch training complete. %d lines processed, %d words added to lexicon.",
									lineCount, wordsAdded);
		}
	}
}