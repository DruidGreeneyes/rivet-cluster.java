package rivet.cluster.spark;

import java.util.Optional;
import java.util.function.Function;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import rivet.core.arraylabels.Labels;
import rivet.core.arraylabels.RIV;
import rivet.util.Util;
import scala.Tuple2;

import static rivet.cluster.spark.Lexicon.getMeanVector;

public class Lexica {
	
	public interface SFn<A, R> extends org.apache.spark.api.java.function.Function<A, R> {}
	public static <A, R> SFn<A, R> sfn (Function<A, R> jfn) {return jfn::apply;}
	
	public static  RIV lexDocument (final JavaRDD<String> document, JavaPairRDD<String, Row> wordLexicon) {
		return document.mapToPair((s) -> new Tuple2<>(s, Optional.empty()))
				.leftOuterJoin(wordLexicon)
				.map(Tuple2::_2)
				.map(Tuple2::_2)
				.map(Util::gOptToJOpt)
				.filter(Optional::isPresent)
				.map(Optional::get)
				.map(sfn(Row.getter("lex")))
				.map(RIV::fromString)
				.reduce(Labels::addLabels)
				.subtract(getMeanVector(wordLexicon));
	}
}
