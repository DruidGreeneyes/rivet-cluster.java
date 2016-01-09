package testing;

import java.io.IOException;

import rivet.cluster.spark.SparkClient;

public class Program {
	public static Log l;
	
	public static void main(String[] args) throws IOException {
		l = new Log("test/programOutput.txt");
		String word = "large";
		String path = "data/sentences";
		try (SparkClient client = new SparkClient("test", "local[3]", "4g", "3g");) {
			print("SparkClient initialized.");
			print("Clearing database 'test'...");
			print(Boolean.toString(client.clearTable()));
			client.trainWordsFromBatch(path);
			print("Lex for word '%s': %s", word, client.getOrMakeWordLex(word).toString());
		} catch (Exception e) {
			print(e.getMessage());
			for (StackTraceElement x : e.getStackTrace())
					print(x.toString());
		}
	}
	
	public static void print (String text) {
		l.log(text);
	}
	public static void print (String fmt, Object...args) {
		l.log(fmt, args);
	}
}
