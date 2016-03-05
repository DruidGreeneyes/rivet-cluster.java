package rivet.cluster.spark;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import rivet.core.arraylabels.Labels;
import rivet.core.arraylabels.RIV;
import rivet.util.Util;
import scala.Tuple2;

import static rivet.cluster.spark.Lexicon.getMeanVector;

public class Lexica {
	
	public interface SFn<A, R> extends org.apache.spark.api.java.function.Function<A, R> {}
	public static final <A, R> SFn<A, R> sfn (Function<A, R> jfn) {return jfn::apply;}
	
	public static final RIV lexDocument (
			final JavaRDD<String> document,
			final WordLexicon wordLexicon) {
		final RIV meanVector = getMeanVector(wordLexicon);
		return document.mapToPair((s) -> new Tuple2<>(s, Optional.empty()))
				.leftOuterJoin(wordLexicon.rdd)
				.map(Tuple2::_2)
				.map(Tuple2::_2)
				.map(Util::gOptToJOpt)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.map(sfn(Row.getter("lex")))
				.map(RIV::fromString)
				.map(RIV::normalize)
				.map(sfn(RIV.subtractor(meanVector)))
				.reduce(Labels::addLabels);
	}
	
	public static final JavaPairRDD<String, RIV> lexDocuments (
			final JavaPairRDD<String, String> documents,
			final WordLexicon wordLexicon) {
		final RIV meanVector = getMeanVector(wordLexicon);
		return documents.flatMapValues((text) -> Arrays.asList(text.split("\\s+")))
				.mapToPair((entry) -> new Tuple2<>(entry._2, entry._1))
				.leftOuterJoin(wordLexicon.rdd)
				.mapToPair((entry) -> entry._2)
				.mapValues(Util::gOptToJOpt)
				.filter((entry) -> entry._2.isPresent())
				.mapValues(Optional::get)
				.mapValues(sfn(Row.getter("lex")))
				.mapValues(RIV::fromString)
				.mapValues(RIV::normalize)
				.mapValues(sfn(RIV.subtractor(meanVector)))
				.reduceByKey(Labels::addLabels);
	}
	
	public static final List<String> assignTopicsToDocument (
			final JavaRDD<String> text,
			final TopicLexicon topicLexicon) {
		final RIV docLex = lexDocument(text, topicLexicon.wordLexicon);
		Comparator<Tuple2<String, RIV>> similarityToDocument = 
				(a, b) -> Double.compare(
						Labels.similarity(a._2, docLex),
						Labels.similarity(b._2, docLex));
		return topicLexicon.rdd
				.mapValues(sfn(Row.getter("lex")))
				.mapValues(RIV::fromString)
				.takeOrdered(10, similarityToDocument)
				.stream()
				.map((entry) -> entry._1)
				.collect(Collectors.toList());
	}
}
