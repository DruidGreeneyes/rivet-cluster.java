package rivet.cluster.spark;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import rivet.cluster.util.Util;
import rivet.cluster.util.XML;
import rivet.core.labels.MapRIV;
import scala.Tuple2;

public final class TopicLexicon extends Lexicon {

    public static final Tuple2<String, String> breakOutTopics(
            final Tuple2<String, String> entry) {
        final String topics = XML.getTagContents(entry._2, "topics");
        final String body = XML.getTagContents(entry._2, "body");
        return new Tuple2<>(topics, body);
    }

    public final WordLexicon wordLexicon;

    public TopicLexicon(final WordLexicon wordLexicon,
            final String hbaseTableName) throws IOException {
        super(wordLexicon.jsc, hbaseTableName);
        this.wordLexicon = wordLexicon;
    }

    public final TopicLexicon trainTopicsFromBatch(
            final JavaPairRDD<String, String> texts) {
        final JavaPairRDD<String, MapRIV> topics = Lexica
                .lexDocuments(texts.mapToPair(TopicLexicon::breakOutTopics),
                        wordLexicon)
                .mapToPair((entry) -> new Tuple2<>(entry._2, entry._1))
                .flatMapValues((topicString) -> Arrays
                        .asList(topicString.split("\\s+")))
                .mapToPair((entry) -> new Tuple2<>(entry._2, entry._1))
                .reduceByKey(MapRIV::add);
        rdd = rdd.fullOuterJoin(topics)
                .mapValues((v) -> new Tuple2<>(Util.gOptToJOpt(v._1),
                        Util.gOptToJOpt(v._2)))
                .mapValues((v) -> Spark.mergeJoinEntry(v));
        return this;
    }

    @Deprecated
    public final TopicLexicon trainTopicsFromFile(final JavaRDD<String> text) {
        return null;
    }

    @Override
    public final String uiTrain(final String path) {
        final File file = new File(path);
        final Instant i = Instant.now();
        if (file.isDirectory()) {
            // log.log("attempting to load files in directory: %s",
            // file.getAbsolutePath());
            final JavaPairRDD<String, String> texts = jsc
                    .wholeTextFiles("file://" + file.getAbsolutePath());
            final long fileCount = texts.count();
            final long startCount = count();
            try {
                trainTopicsFromBatch(texts);
            } catch (final Exception e) {
                e.printStackTrace();
                return e.getMessage();
            }
            final long wordsAdded = count() - startCount;
            return String.format(
                    "Batch training complete. %d files processed, %d topics added to lexicon.\nElapsed time: %s",
                    fileCount, wordsAdded, Duration.between(i, Instant.now()));
        } else {
            final JavaRDD<String> text = jsc.textFile(path);
            final long lineCount = text.count();
            final long startCount = count();
            try {
                trainTopicsFromFile(text);
            } catch (final Exception e) {
                e.printStackTrace();
                return e.getMessage();
            }
            final long wordsAdded = count() - startCount;
            return String.format(
                    "Batch training complete. %d lines processed, %d topics added to lexicon.\nElapsed time: %s",
                    lineCount, wordsAdded, Duration.between(i, Instant.now()));
        }
    }

}
