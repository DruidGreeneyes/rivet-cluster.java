package testing;

import java.io.IOException;
import rivet.cluster.spark.SparkClient;

public class Program {
	public static void main(String[] args) throws IOException {
		String word = "large";
		String path = "data/sentences";
		try (SparkClient client = new SparkClient("test");) {
			print("SparkClient initialized.");
			print("DB cleared: " + client.clearTable());
			
			//client.trainWordsFromBatch(path);
			//print("Lex for word '" + word + "': " + client.getOrMakeWordLex(word));
		} /*catch (Exception e) {
			System.out.println(e.getMessage());
			Arrays.stream(e.getStackTrace()).forEach((x) ->
					System.out.println(x));
		}*/
	}
	
	public static void print (String text) {
		System.out.println(text);
	}
}
