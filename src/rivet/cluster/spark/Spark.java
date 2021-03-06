package rivet.cluster.spark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import rivet.cluster.hbase.HBase;
import rivet.cluster.util.Util;
import rivet.core.labels.MapRIV;
import scala.Tuple2;

public class Spark {

    public static final Tuple2<Optional<?>, Optional<?>> EMPTY_ENTRY = new Tuple2<>(
            Optional.empty(), Optional.empty());

    public static <R> Tuple2<ImmutableBytesWritable, R> keyToIBW(
            final Tuple2<String, R> entry) {
        return new Tuple2<>(new ImmutableBytesWritable(Bytes.toBytes(entry._1)),
                entry._2);
    }

    public static Row mergeJoinEntry(
            final Tuple2<Optional<Row>, Optional<MapRIV>> entry) {
        if (entry._1.isPresent() && !entry._2.isPresent())
            return entry._1.get();

        final Row row = entry._1.orElse(new Row());
        final Optional<MapRIV> oldRIV = Optional.ofNullable(row.get("lex"))
                .map(MapRIV::fromString);
        final MapRIV newRIV = Util.mergeOptions(entry._2, oldRIV, MapRIV::add)
                .orElseThrow(IndexOutOfBoundsException::new);
        row.put("lex", newRIV.toString());
        if (row.size() < 1)
            throw new RuntimeException(
                    "MergeJoinEntry has failed! : " + row.toString());
        return row;
    }

    public static SparkConf newSparkConf() {
        return new SparkConf().setAppName("Rivet");
    }

    public static SparkConf newSparkConf(final List<Setting> settings) {
        final Optional<Setting> hasMaster = settings.stream()
                .filter((s) -> s._1.equals("master")).findFirst();
        final String master = hasMaster.isPresent()
                ? settings.remove(settings.indexOf(hasMaster.get()))._2
                : "local[*]";
        return newSparkConf(master, settings);
    }

    public static SparkConf newSparkConf(final Setting... settings) {
        return newSparkConf(Arrays.asList(settings));
    }

    public static SparkConf newSparkConf(final String master,
            final List<Setting> settings) {
        final SparkConf sparkConf = newSparkConf().setMaster(master);
        settings.forEach((entry) -> sparkConf.set(entry._1, entry._2));
        return sparkConf;
    }

    public static SparkConf newSparkConf(final String master,
            final Map<String, String> settings) {
        final SparkConf res = new SparkConf().setAppName("Rivet")
                .setMaster(master);
        settings.forEach(res::set);
        return res;
    }

    public static SparkConf newSparkConf(final String master,
            final Setting... settings) {
        return newSparkConf(master, Arrays.asList(settings));
    }

    public static TopicLexicon openTopicLexicon(final JavaSparkContext jsc,
            final String wordLexName, final String topicLexName)
            throws IOException {
        return openTopicLexicon(openWordLexicon(jsc, wordLexName),
                topicLexName);
    }

    public static TopicLexicon openTopicLexicon(final WordLexicon wordLexicon,
            final String name) throws IOException {
        return new TopicLexicon(wordLexicon, name);
    }

    public static WordLexicon openWordLexicon(final JavaSparkContext jsc,
            final String name) throws IOException {
        return new WordLexicon(jsc, name);
    }

    public static Tuple2<ImmutableBytesWritable, Put> prepareEntryForStorage(
            final Tuple2<String, Row> entry) {
        final ImmutableBytesWritable key = HBase.stringToIBW(entry._1);
        Put val;
        try {
            val = HBase.newPut(key, entry._2);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Cannot prepare entry for storage: " + entry.toString());
        }
        return new Tuple2<>(key, val);
    }

    public static Tuple2<String, String> prepareInputEntry(
            final Tuple2<ImmutableBytesWritable, Result> entry) {
        return new Tuple2<>(HBase.ibwToString(entry._1),
                new Row(entry._2).get("text"));
    }

    public static Tuple2<String, Row> prepareLexiconEntry(
            final Tuple2<ImmutableBytesWritable, Result> entry) {
        return new Tuple2<>(HBase.ibwToString(entry._1), new Row(entry._2));
    }
}
