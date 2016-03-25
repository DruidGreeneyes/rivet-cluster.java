package rivet.cluster.spark;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URI;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;

import rivet.util.Counter;
import scala.Tuple2;

public class FileProcessor implements Closeable {
	
	private final JavaSparkContext jsc;
	
	public FileProcessor (JavaSparkContext jsc) { this.jsc = jsc; }
	public FileProcessor (SparkConf conf) { this(new JavaSparkContext(conf)); }
	
	public void processFileBatch (
			Function<String, String> fun,
			JavaPairRDD<String, String> files) {
		String s = files.first()._1;
		String d = s.substring(0, s.lastIndexOf("/")) + "/processed/";
		new File(d).mkdirs();
		files.mapValues(fun)
			.saveAsTextFile(d);
		/*files.mapValues(fun)
			.foreach((entry) -> {
						String path = entry._1;
						String filename = path.substring(path.lastIndexOf("/") + 1, path.length());
						File f = new File(URI.create(d + filename));						
						try {
							f.createNewFile();
							FileWriter fw = new FileWriter(f);
							fw.write(entry._2);
							fw.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					});*/
	}
	public void processFileBatch (
			Function<String, String> fun,
			String path) {
		this.processFileBatch(fun, this.jsc.wholeTextFiles(path));
	}
	
	public static String processToSentences (String text) {
		try (BufferedReader br = new BufferedReader(new StringReader(text))) {
			return br.lines()
					.collect(Collectors.joining(" "));
		} catch (IOException e) {
			e.printStackTrace();
			return "ERROR!";
		}
	}
	
	public void processBatchToSentences (String path) {
		this.processFileBatch(FileProcessor::processToSentences, path);
	}
	
	public static String processSGMLToSentences (String text) {
		try (BufferedReader br = new BufferedReader (new StringReader(text))) {
			Counter c = new Counter();
			Pattern bodyBlocks = Pattern.compile("(<BODY>)|(&#3;</BODY>)");
			Pattern acronyms1 = Pattern.compile("(?<=[A-Z]\\.)([A-Z])\\.");
			Pattern acronyms2 = Pattern.compile("([A-Z])\\.(?=[A-Z])");
			Pattern paots = Pattern.compile("(?<=(\\S)\\.{1,3}(\"?))\\s+(?=(\"?[A-Z]))");
			Predicate<String> oneWordLine = Pattern.compile("^\\s*\\w+\\s*$").asPredicate().negate();
			return bodyBlocks.splitAsStream(br.lines().map(String::trim).collect(Collectors.joining(" ")))
						.map((str) -> new Tuple2<>(c.inc(), str))
						.filter((e) -> e._1 % 2 == 0)
						.map(Tuple2::_2)
						.map((str) -> acronyms1.matcher(str).replaceAll("$1"))
						.map((str) -> acronyms2.matcher(str).replaceAll("$1"))
						.flatMap((str) -> paots.splitAsStream(str))
						.map((str) -> str.replace(".", ""))
						.filter(oneWordLine)
						.collect(Collectors.joining("\n"));
		} catch (IOException e) {
			e.printStackTrace(); 
			return "ERROR!";
		}
	}
	
	public void processSGMLBatchToSentences (String path) throws IOException {
		this.processFileBatch(FileProcessor::processSGMLToSentences, path);
	}
	
	public String uiProcess (String path) {
		File f = new File(path);
		JavaPairRDD<String, String> files = this.jsc.wholeTextFiles("file://" + f.getAbsolutePath());
		long count = files.count();
		if (files.first()._1.endsWith(".sgm") || files.first()._1.endsWith(".sgml"))
			processFileBatch(
					FileProcessor::processSGMLToSentences,
					files);
		else
			processFileBatch(
					FileProcessor::processToSentences,
					files);
		return String.format("Processing complete: %s files processed", count);
	}
	
	@Override
	public void close() throws IOException {
		this.jsc.close();		
	}
}
