import java.io.IOException;

import rivet.cluster.spark.SparkClient;
import rivet.core.RIV;

public class Program {
	public static void main(String[] args) throws IOException {
		String word = "text";
		try (SparkClient client = new SparkClient("test")) {
			print("SparkClient initialized.");
			print("getWordLex " + word + ": " + client.getWordLex(word));
			RIV ind = client.getWordInd(word);
			print("Ind for " + word + ": " + ind.toString());
			print("hbcSetWordLex: " + client.rddSetWordLex(word, ind).isPresent());
			print("getWordLex " + word + ": " + client.getWordLex(word));
		}
	}
	
	public static void print (String text) {
		System.out.println(text);
	}
}
