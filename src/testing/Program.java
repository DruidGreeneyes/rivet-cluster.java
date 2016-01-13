package testing;

import java.io.IOException;
import java.time.Instant;

import rivet.Util;
import rivet.cluster.spark.FileProcessor;
import rivet.cluster.spark.SparkClient;
import rivet.core.HashLabels;
import rivet.core.RIV;

public class Program {
	public static final Log log = new Log("test/programOutput.txt");
	
	public static void main(final String[] args) throws IOException {
		Instant t = Instant.now();
		testSGMLProcessing();
		log.log("Test Completed in %s", Util.timeSince(t));
	}
	
	public static void testLexiconTraining() throws IOException {
		String word = "large";
		String path = "/home/josh/Copy/Programming/JAVA/src/rivet.java/data/sentences";
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
	
	public static void testRandomPermutations() {
		print("Testing random permutations...");
		RIV big = HashLabels.generateLabel(100, 10, "big");
		print("big: " + big.toString());
		RIV bigPlusOne = big.permuteFromZero(1);
		print("big + 1: " + bigPlusOne.toString());
		RIV bigMatch = bigPlusOne.permuteToZero(1);
		print("big: " + bigMatch.toString());
		print(big.equals(bigMatch));
	}
	
	public static void testSGMLProcessing() {
		String path = "/home/josh/Copy/Programming/JAVA/src/rivet.java/data/reuters";
		try (FileProcessor fp = new FileProcessor("local[3]", "4g", "3g")) { 
			fp.processSGMLBatchToSentences("data/reuters");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void print (Object obj) {
		log.log(obj);
	}
	public static void print (String fmt, Object...args) {
		log.log(fmt, args);
	}
}
