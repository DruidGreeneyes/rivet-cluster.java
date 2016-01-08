package testing;

import java.io.IOException;

import rivet.cluster.spark.SparkClient2;

public class Program {
	public static Log l;
	
	public static void main(String[] args) throws IOException {
		l = new Log("test/testOutput.txt");
		String word = "large";
		String path = "/home/josh/Copy/Programming/JAVA/src/rivet.java/data/sentences";
		try (SparkClient2 client = new SparkClient2("test", "local[3]", "4g", "3g");) {
			print("SparkClient initialized.");
			print("Database cleared 'test': " + client.clearTable());
			client.trainWordsFromBatch(path);
			print("Lex for word '" + word + "': " + client.getOrMakeWordLex(word));
		} /*catch (Exception e) {
			System.out.println(e.getMessage());
			Arrays.stream(e.getStackTrace())
				.forEach((x) -> System.out.println(x));
		}*/
	}
	
	public static void print (String text) throws IOException {
		l.log(text);
	}
}
