package rivet.cluster.spark;

import static rivet.cluster.spark.Lexicon.getMeanVector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import rivet.cluster.util.Util;
import rivet.core.labels.MapRIV;
import rivet.core.labels.RIVs;
import scala.Tuple2;

public class Lexica {

    public interface SBFn<A, B, R>
            extends org.apache.spark.api.java.function.Function2<A, B, R> {
    }

    public interface SFn<A, R>
            extends org.apache.spark.api.java.function.Function<A, R> {
    }

    public static final List<String> assignTopicsToDocument(
            final JavaRDD<String> text, final TopicLexicon topicLexicon) {
        final MapRIV docLex = lexDocument(text, topicLexicon.wordLexicon);
        final Comparator<Tuple2<String, MapRIV>> similarityToDocument = (a,
                b) -> Double.compare(RIVs.similarity(a._2, docLex),
                        RIVs.similarity(b._2, docLex));
        return topicLexicon.rdd.mapValues(sfn(Row.getter("lex")))
                .mapValues(MapRIV::fromString)
                .takeOrdered(10, similarityToDocument).stream()
                .map((entry) -> entry._1).collect(Collectors.toList());
    }

    public static final JavaPairRDD<String, List<Double>> compareDocumentBatch(
            final WordLexicon wordLexicon,
            final JavaPairRDD<String, String> documents) {
        final JavaPairRDD<String, MapRIV> rivs = lexDocuments(documents,
                wordLexicon);
        final List<MapRIV> rivList = rivs.values().collect();
        return rivs.mapValues(
                (r) -> rivList.stream().map((s) -> RIVs.similarity(r, s))
                        .collect(Collectors.toList()));
    }

    public static final double compareDocuments(final WordLexicon wordLexicon,
            final JavaRDD<String> docA, final JavaRDD<String> docB) {
        final MapRIV rivA = lexDocument(docA, wordLexicon);
        final MapRIV rivB = lexDocument(docB, wordLexicon);
        return RIVs.similarity(rivA, rivB);
    }

    public static final MapRIV lexDocument(final JavaRDD<String> document,
            final WordLexicon wordLexicon) {
        final MapRIV meanVector = getMeanVector(wordLexicon);
        return document.mapToPair((s) -> new Tuple2<>(s, Optional.empty()))
                .leftOuterJoin(wordLexicon.rdd).map(Tuple2::_2).map(Tuple2::_2)
                .map(Util::gOptToJOpt).filter(Optional::isPresent)
                .map(Optional::get).map(sfn(Row.getter("lex")))
                .map(MapRIV::fromString).map(MapRIV::normalize)
                .map(subtractor(meanVector)).reduce(MapRIV::destructiveAdd);
    }

    public static final JavaPairRDD<String, MapRIV> lexDocuments(
            final JavaPairRDD<String, String> documents,
            final WordLexicon wordLexicon) {
        final MapRIV meanVector = getMeanVector(wordLexicon);
        return documents
                .flatMapValues((text) -> Arrays.asList(text.split("\\s+")))
                .mapToPair((entry) -> new Tuple2<>(entry._2, entry._1))
                .leftOuterJoin(wordLexicon.rdd).mapToPair((entry) -> entry._2)
                .mapValues(Util::gOptToJOpt)
                .filter((entry) -> entry._2.isPresent())
                .mapValues(Optional::get).mapValues(sfn(Row.getter("lex")))
                .mapValues(MapRIV::fromString).mapValues(MapRIV::normalize)
                .mapValues(subtractor(meanVector))
                .reduceByKey(MapRIV::destructiveAdd);
    }

    public static final <A, R> SFn<A, R> sfn(final Function<A, R> jfn) {
        return jfn::apply;
    }

    private final static SFn<MapRIV, MapRIV> subtractor(final MapRIV sub) {
        return r -> r.subtract(sub);
    }
}
