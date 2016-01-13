package rivet.cluster.spark;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Stack;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;

import rivet.Util;
import rivet.util.ReadTable;
import testing.Log;

public class FileProcessor implements Closeable {
	private final static Log log = new Log("test/fileProcessorOutput.txt");
	
	private final JavaSparkContext jsc;
	
	public FileProcessor (final String master, final String hostRam, final String workerRam) {
		this.jsc = 
				new JavaSparkContext(
						new SparkConf()
							.setAppName("Rivet")
							.setMaster(master)
							.set("spark.driver.memory", hostRam)
							.set("spark.executor.memory", workerRam));
	}
	
	public void processFileBatch (
			Function<PortableDataStream, String> fun,
			JavaPairRDD<String, PortableDataStream> files) {
		files.mapValues(fun)
			.foreach(
					(entry) -> {
						File f = new File(entry._1 + ".processed");
						try {
							f.createNewFile();
							FileWriter fw = new FileWriter(f);
							fw.write(entry._2);
							fw.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
	}
	public void processFileBatch (
			Function<PortableDataStream, String> fun,
			String path) {
		this.processFileBatch(fun, this.jsc.binaryFiles(path));
	}
	
	public static String processToSentences (PortableDataStream text) {
		String res = "Error!";
		try (InputStreamReader isr = new InputStreamReader(text.open());
				BufferedReader br = new BufferedReader(isr)) {
			res = br.lines()
					.map((s) -> s + " ")
					.reduce("", Util::concatStrings);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public void processBatchToSentences (String path) {
		this.processFileBatch(FileProcessor::processToSentences, path);
	}
	
	public static int readFrom(InputStreamReader isr) {
		int r = -1;
		try {
			r = isr.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}
	
	public static String processSGMLToSentences (PortableDataStream text) {
		String res = "Error!";
		try (InputStreamReader isr = new InputStreamReader(text.open())) {
			ReadTable readTable = new ReadTable();
			Stream.generate(() -> readFrom(isr))
						.peek(readTable::act)
						.anyMatch((x) -> x == -1);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public void processSGMLBatchToSentences (String path) throws IOException {
		this.processFileBatch(FileProcessor::processSGMLToSentences, path);
	}
	
	@Override
	public void close() throws IOException {
		this.jsc.close();		
	}
}
