package testing;

import static rivet.util.Util.setting;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import rivet.cluster.spark.FileProcessor;
import rivet.cluster.spark.SparkClient;
import rivet.core.hashlabels.HashLabels;
import rivet.core.hashlabels.RIV;
import rivet.util.Util;
import scala.Tuple2;

public class Program {
	public static final Log log = new Log("test/programOutput.txt");
	
	public static void main(final String[] args) throws IOException {
		Instant t = Instant.now();
		testPermutations();
		log.log("Test Completed in %s", Util.timeSince(t));
	}
	
	public static void testPermutations() {
		print("Testing random permutations...");
		RIV big = HashLabels.generateLabel(100, 10, "big");
		print("big: " + big.toString());
		Tuple2<int[], int[]> permutations = HashLabels.generatePermutations(100);
		RIV bigPlusOne = big.permute(permutations, 1);
		print("big + 1: " + bigPlusOne.toString());
		RIV bigMatch = bigPlusOne.permute(permutations, -1);
		print("big: " + bigMatch.toString());
		print(big.equals(bigMatch));
	}
	
	public static void testSGMLProcessing() {
		String path = "data/reuters";
		try (FileProcessor fp = new FileProcessor("local[3]", "4g", "3g")) { 
			fp.processSGMLBatchToSentences(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void testFileCreation() throws IOException {
		File f = new File("data/reuters/test.txt");
		f.createNewFile();
	}
	
	public static void testRDDOnlyProcessing() throws IOException {
		String word = "large";
		String path = "data/enron/sentence";
		try (SparkClient client = new SparkClient(
				"test",
				"local[3]", 
				setting("spark.driver.memory", "4g"),
				setting("spark.executor.memory", "3g"))) {
			print("SparkClient initialized.");
			RIV lex = client.getOrMakeWordLex(word);
			print("Lex for word '%s': %s", word, lex);
			print("Clearing database 'test'...");
			print(client.clear().count());
			print("Training lexicon from text files in folder: %s", path);
			client.trainWordsFromSentenceBatch(path);
			RIV lex2 = client.getOrMakeWordLex(word);
			print("Lex for word '%s': %s", word, lex2);
			print("Are lexes equal? " + lex.equals(lex2));
			client.write();
		} catch (Exception e) {
			print(e.getMessage());
			for (StackTraceElement x : e.getStackTrace())
					print(x.toString());
		}
	}
	
	public static void print (Object obj) {
		log.log(obj);
	}
	public static void print (String fmt, Object...args) {
		log.log(fmt, args);
	}
}
