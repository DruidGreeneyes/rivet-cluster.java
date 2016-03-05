package rivet.cluster.spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import rivet.core.arraylabels.Labels;
import rivet.core.arraylabels.RIV;
import rivet.util.Util;
import scala.Tuple2;
import testing.Log;

public final class TopicLexicon extends Lexicon {
	Log log = new Log("test/topicLexiconOutput.txt");
	private static final String CATBREAK= "-==-";
	
	public final WordLexicon wordLexicon;
	
	public TopicLexicon (final WordLexicon wordLexicon, final String hbaseTableName) throws IOException {
		super(wordLexicon.jsc, hbaseTableName);
		this.wordLexicon = wordLexicon;
	}
	
	public static final Tuple2<String, String> breakOutTopics (final Tuple2<String, String> entry) {
		String[] breaked = entry._2.split("\\s+" + CATBREAK + "\\s+");
		return new Tuple2<>(breaked[0], breaked[1]);
	}
	
	public final TopicLexicon trainTopicsFromBatch (final JavaPairRDD<String, String> texts) {
		JavaPairRDD<String, RIV> topics = 
				Lexica.lexDocuments(
					texts.mapToPair(TopicLexicon::breakOutTopics),
					this.wordLexicon)				
				.mapToPair((entry) -> new Tuple2<>(entry._2, entry._1))
				.flatMapValues((topicString) -> Arrays.asList(topicString.split("\\s+")))
				.mapToPair((entry) -> new Tuple2<>(entry._2, entry._1))
				.reduceByKey(Labels::addLabels);
		this.rdd = this.rdd.fullOuterJoin(topics)
				.mapValues((v) -> new Tuple2<>(
						Util.gOptToJOpt(v._1),
						Util.gOptToJOpt(v._2)))
				.mapValues((v) -> Spark.mergeJoinEntry(v));
		return this;
	}
	
	
	@Deprecated
	public final TopicLexicon trainTopicsFromFile(JavaRDD<String> text) {
		return null;
	}

	@Override
	public final String uiTrain(String path) {
		File file = new File(path);
		if (file.isDirectory()){
			log.log("attempting to load filed in directory: %s", file.getAbsolutePath());
			JavaPairRDD<String, String> texts = jsc.wholeTextFiles("file://" + file.getAbsolutePath());
			long fileCount = texts.count();
			long startCount = this.count();
			try {
				this.trainTopicsFromBatch(texts);
			} catch (Exception e) {
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
				this.trainTopicsFromFile(text);
			} catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}
			long wordsAdded = this.count() - startCount;
			return String.format("Batch training complete. %d lines processed, %d words added to lexicon.",
									lineCount, wordsAdded);
		}
	}
	
	
}
