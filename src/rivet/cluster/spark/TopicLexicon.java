package rivet.cluster.spark;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;

public class TopicLexicon extends Lexicon {
	public TopicLexicon (final JavaSparkContext jsc, final String hbaseTableName) throws IOException {
		super(jsc, hbaseTableName);
	}

	@Override
	public String uiTrain(String path, JavaSparkContext jsc) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}
