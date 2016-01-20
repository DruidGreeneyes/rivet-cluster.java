package rivet.cluster.spark;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Client extends JavaSparkContext {
	public Client(SparkConf conf) {super(conf);}
	
	public WordLexicon openWordLexicon (String name) throws IOException {
		return new WordLexicon(this, name);
	}
	
	public TopicLexicon openTopicLexicon (String name) throws IOException {
		return new TopicLexicon(this, name);
	}
}
