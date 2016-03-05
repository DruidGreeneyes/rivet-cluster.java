package rivet.program;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;

import rivet.cluster.spark.FileProcessor;
import rivet.cluster.spark.Lexicon;
import rivet.cluster.spark.Setting;
import rivet.cluster.spark.Spark;
import rivet.persistence.hbase.HBase;

public final class MethodIndex implements Closeable{
	private static final String SPARK_CONF = "conf/spark.conf";
	
	private final JavaSparkContext jsc;
	public final List<Method> methods;
	
	public MethodIndex() {
		this.jsc = 
				new JavaSparkContext(
						Spark.newSparkConf(
								loadSettings(SPARK_CONF)));
		methods = Arrays.stream(MethodIndex.class.getDeclaredMethods())
				.filter((m) -> Modifier.isPublic(m.getModifiers()))
				.collect(toList());
		
	}
	
	@Override
	public final void close() {this.jsc.close();}
	
	private static final List<Setting> loadSettings(String path) {
		List<String> lines;
		try {
			lines = Files.readAllLines(Paths.get(path));
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to load spark configuration!\n" + e.getMessage());
		}
		List<Setting> res = lines.stream()
								.map((l) -> l.replaceAll("\\s+", ""))
								.map((l) -> l.split(":", 2))
								.filter((l) -> l.length == 2)
								.map((l) -> new Setting(l[0], l[1]))
								.collect(Collectors.toList());
		System.out.println("Settings: ");
		res.forEach((s) ->
			System.out.print(s.toString() + " "));
		System.out.println();
		return res;
	}
	
	private final Lexicon loadLexicon(String type, String name) {
		Lexicon lexicon;
		try {
			switch (type) {
			case "word"  :
			case "words" : lexicon = Spark.openWordLexicon(jsc, name); break;
			//case "topic" :
			//case "topics": lexicon = Spark.openTopicLexicon(jsc, name); break;

			default      : throw new RuntimeException("Not an applicable lexicon data type: " + type);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to load Lexicon!" + e.getMessage());
		}
		return lexicon;
	}
	
	public final String train (String dataType, String lexiconName, String path) {
		Lexicon lexicon = this.loadLexicon(dataType, lexiconName);
		try {
			String res = lexicon.uiTrain(path);
			lexicon.write();
			return res;
		} catch (IOException e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}
	
	public final String clear (String dataType, String lexiconName) {			
		Lexicon lexicon = this.loadLexicon(dataType, lexiconName);
		try {
			HBase.clearTable(lexiconName);
			return Long.toString(lexicon.count());
		} catch (IOException e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}
	
	public final String count (String dataType, String lexiconName) {
		return Long.toString(
				this.loadLexicon(dataType, lexiconName)
					.count());
	}
	
	public final String processDirectory (String path) {
		try (FileProcessor p = new FileProcessor(this.jsc)) {
			return p.uiProcess(path);
		} catch (IOException e) {
			e.printStackTrace();
			return e.getMessage();
		}
	}
}
