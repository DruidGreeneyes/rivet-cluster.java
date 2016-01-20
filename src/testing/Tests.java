package testing;

import static rivet.util.Util.setting;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import rivet.cluster.spark.FileProcessor;
import rivet.cluster.spark.Spark;
import rivet.cluster.spark.Client;
import rivet.cluster.spark.WordLexicon;
import rivet.core.arraylabels.Labels;
import rivet.core.arraylabels.RIV;
import rivet.program.REPL;
import rivet.util.Util;
import scala.Tuple2;

public class Tests {
	public static final Log log = new Log("test/programOutput.txt");
	public static String path = "data/reuters";
	
	public static void main(final String[] args) throws IOException {
		Instant t = Instant.now();
		testRepl(args);
		//testRDDOnlyProcessing((args.length>0)?args[0]:path);
		log.log("Test Completed in %s", Util.timeSince(t));
	}
	
	public static void testRepl(String[] args) {
		print("Calling repl...");
		REPL.main(args);
	}
	
	public static void testPermutations() {
		print("Testing random permutations...");
		RIV big = Labels.generateLabel(100, 10, "big");
		print("big: " + big.toString());
		Tuple2<int[], int[]> permutations = Labels.generatePermutations(100);
		RIV bigPlusOne = big.permute(permutations, 1);
		print("big + 1: " + bigPlusOne.toString());
		print(big.equals(bigPlusOne));
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
	
	public static void testRDDOnlyProcessing(String path) throws IOException {
		String word = "large";
		try (JavaSparkContext jsc= Spark.newClient(
				"local[3]", 
				setting("spark.driver.memory", "4g"),
				setting("spark.executor.memory", "3g"))) {
			print("SparkContext initialized.");
			WordLexicon lexicon = new WordLexicon(jsc, "test");
			print("Clearing database 'test'...");
			print(lexicon.clear().count());
			print("Training lexicon from text files in folder: %s", path);
			JavaPairRDD<String, String> texts = jsc.wholeTextFiles(path);
			lexicon.trainWordsFromSentenceBatch(texts);
			RIV lex2 = lexicon.getOrMakeLex(word);
			print("Lex for word '%s': %s", word, lex2);
			lexicon.write();
		} catch (Exception e) {
			print(e.getMessage());
			for (StackTraceElement x : e.getStackTrace())
					print(x.toString());
		}
	}
	
	public static void testHBaseTableCreation() throws IOException {
		try (Client client = Spark.newClient(
				"local[3]",
				setting("spark.driver.memory", "4g"),
				setting("spark.executor.memory", "3g"))) {
			WordLexicon lexicon = client.openWordLexicon("otherTest");			
		}
	}
	
	public static void print (Object obj) {
		log.log(obj);
	}
	public static void print (String fmt, Object...args) {
		log.log(fmt, args);
	}
}
