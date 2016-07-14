package rivet.cluster.spark;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import rivet.cluster.hbase.HBase;
import rivet.cluster.util.Util;
import rivet.core.labels.MapRIV;
import rivet.core.labels.RIVs;
import rivet.core.vectorpermutations.Permutations;
import scala.Tuple2;

public class WordLexicon extends Lexicon {

    private static List<Tuple2<String, MapRIV>> breakAndGetContextRIVs(
            final Tuple2<String, String> textEntry, final Integer size,
            final Integer k, final Integer cr) {
        final String text = textEntry._2;
        final List<String> words = Arrays.asList(text.split("\\s+"));
        final Integer count = words.size();
        return Util.range(count).mapToObj((index) -> {
            return getContextRIV(words, index, size, k, cr);
        }).collect(Collectors.toList());
    }

    private static List<Tuple2<String, MapRIV>> breakAndGetSentenceRIVs(
            final Tuple2<String, String> textEntry, final Integer size,
            final Integer k) {
        final String text = textEntry._2;
        final List<String> sentences = Arrays.asList(text.split("(\\n|\\r)+"));
        final Permutations permutations = Permutations.generate(size);
        return sentences.stream()
                .flatMap((sentence) -> getPermutedSentenceRIVs(
                        splitAndRemoveEmpties(sentence, "\\s+"), size, k,
                        permutations))
                .collect(Collectors.toList());
    }

    private static Tuple2<String, MapRIV> getContextRIV(
            final List<String> tokens, final Integer index, final Integer size,
            final Integer k, final Integer cr) {
        final Integer count = tokens.size();
        return new Tuple2<>(tokens.get(index),
                Util.butCenter(Util.rangeNegToPos(cr), cr)
                        .filter(n -> 0 <= n && n < count)
                        .mapToObj(n -> tokens.get(n))
                        .map(word -> getInd(size, k, word))
                        .reduce(new MapRIV(size), MapRIV::destructiveAdd));
    }

    private static Tuple2<String, MapRIV> getPermutedContextRIV(
            final String[] context, final Integer index, final Integer size,
            final Integer k, final Permutations permutations) {
        final Integer count = context.length;
        final MapRIV riv = Util.range(count).filter((i) -> !index.equals(i))
                .mapToObj((i) -> getInd(size, k, context[i])
                        .permute(permutations, i - index))
                .reduce(new MapRIV(size), MapRIV::destructiveAdd);
        return new Tuple2<>(context[index], riv);
    }

    private static Stream<Tuple2<String, MapRIV>> getPermutedSentenceRIVs(
            final String[] sentence, final Integer size, final Integer k,
            final Permutations permutations) {
        return Util.range(sentence.length)
                .mapToObj((index) -> getPermutedContextRIV(sentence, index,
                        size, k, permutations));
    }

    private static String[] splitAndRemoveEmpties(final String str,
            final String regex) {
        String[] res = new String[0];
        for (final String s : str.split(regex))
            if (s.length() != 0)
                res = ArrayUtils.add(res, s);
        return res;
    }

    private final Integer cr;

    // Class necessities
    public WordLexicon(final JavaSparkContext jsc, final String hbaseTableName)
            throws IOException {
        super(jsc, hbaseTableName);
        cr = getMetadata(CR_COLUMN).orElse(DEFAULT_CR);
    }

    public WordLexicon batchTrainer(final JavaPairRDD<String, String> texts,
            final PairFlatMapFunction<Tuple2<String, String>, String, MapRIV> trainer) {
        final JavaPairRDD<String, MapRIV> lexes = texts.flatMapToPair(trainer)
                .reduceByKey(MapRIV::add);
        rdd = rdd.fullOuterJoin(lexes)
                .mapValues((v) -> new Tuple2<>(Util.gOptToJOpt(v._1),
                        Util.gOptToJOpt(v._2)))
                .mapValues((v) -> Spark.mergeJoinEntry(v));
        return this;
    }

    public double compareWords(final String wordA, final String wordB) {
        return RIVs.similarity(getLexOrError(wordA), getLexOrError(wordB));
    }

    // Training
    public WordLexicon trainer(final JavaRDD<String> text,
            final PairFlatMapFunction<String, String, MapRIV> trainer) {
        final JavaPairRDD<String, MapRIV> lexes = text.flatMapToPair(trainer)
                .reduceByKey(MapRIV::add);
        rdd = rdd.fullOuterJoin(lexes)
                .mapValues((v) -> new Tuple2<>(Util.gOptToJOpt(v._1),
                        Util.gOptToJOpt(v._2)))
                .mapValues((v) -> Spark.mergeJoinEntry(v));
        return this;
    }

    public WordLexicon trainWordsFromBatch(
            final JavaPairRDD<String, String> tokenizedTexts) {
        final Integer size = this.size;
        final Integer k = this.k;
        final Integer cr = this.cr;
        return batchTrainer(tokenizedTexts,
                (textEntry) -> breakAndGetContextRIVs(textEntry, size, k, cr));
    }

    public WordLexicon trainWordsFromSentenceBatch(
            final JavaPairRDD<String, String> tokenizedTexts) {
        final Integer size = getSize();
        final Integer k = getK();
        return batchTrainer(tokenizedTexts,
                (textEntry) -> breakAndGetSentenceRIVs(textEntry, size, k));
    }

    public WordLexicon trainWordsFromSentenceFile(final JavaRDD<String> text) {
        final Integer size = getSize();
        final Integer k = getK();
        final Permutations permutations = Permutations.generate(size);
        return trainer(text,
                (line) -> getPermutedSentenceRIVs(line.split("\\s+"), size, k,
                        permutations).collect(Collectors.toList()));
    }

    /*
     * public String uiTrain(String path) { File file = new File(path); Instant
     * i = Instant.now(); if (file.isDirectory()){
     * //log.log("attempting to load files in directory: %s",
     * file.getAbsolutePath()); JavaPairRDD<String, String> texts =
     * jsc.wholeTextFiles("file://" + file.getAbsolutePath()); long fileCount =
     * texts.count(); long startCount = this.count(); try {
     * this.trainWordsFromSentenceBatch(texts); } catch (Exception e) {
     * e.printStackTrace(); return e.getMessage(); } long wordsAdded =
     * this.count() - startCount; return String.
     * format("Batch training complete. %d files processed, %d words added to lexicon.\nElapsed time: %s"
     * , fileCount, wordsAdded, Duration.between(i, Instant.now())); } else {
     * JavaRDD<String> text = jsc.textFile(path); long lineCount = text.count();
     * long startCount = this.count(); try {
     * this.trainWordsFromSentenceFile(text); } catch (Exception e) {
     * e.printStackTrace(); return e.getMessage(); } long wordsAdded =
     * this.count() - startCount; return String.
     * format("Batch training complete. %d lines processed, %d words added to lexicon.\nElapsed time: %s"
     * , lineCount, wordsAdded, Duration.between(i, Instant.now())); } }
     */

    @Override
    public String uiTrain(final String inputSetName) {
        try {
            if (!HBase.tableExists(inputSetName))
                return "Could not find input set: " + inputSetName;
        } catch (final IOException e) {
            e.printStackTrace();
            return "Error encountered while checking table existence: "
                    + e.getMessage();
        }
        final Instant i = Instant.now();
        final JavaPairRDD<String, String> inputSet = jsc
                .newAPIHadoopRDD(HBase.newConf(inputSetName),
                        TableInputFormat.class, ImmutableBytesWritable.class,
                        Result.class)
                .mapToPair(Spark::prepareInputEntry);
        final long inputCount = inputSet.count();
        final long startCount = count();
        try {
            trainWordsFromSentenceBatch(inputSet);
        } catch (final Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
        final long wordsAdded = count() - startCount;
        return String.format(
                "Batch training complete. %d files processed, %d words added to lexicon.\nElapsed time: %s",
                inputCount, wordsAdded, Duration.between(i, Instant.now()));
    }
}