package rivet.program;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hbase.util.Bytes;

import rivet.cluster.spark.Client;
import rivet.cluster.spark.Lexicon;
import rivet.cluster.spark.Spark;
import scala.Tuple2;

public final class MethodIndex {
	private static final String SPARK_CONF = "conf/spark.conf";
	private static final Tuple2<String, String>[] loadSettings(String path) throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(path));
		Tuple2<String, String>[] res = (Tuple2<String, String>[]) new Tuple2[lines.size()];
		lines.stream()
			.map((l) -> l.replaceAll("\\s+", ""))
			.map((l) -> l.split(":", 2))
			.filter((l) -> l.length == 2)
			.map((l) -> new Tuple2<>(l[0], l[1]))
			.collect(Collectors.toList())
			.toArray(res);
		for (Tuple2<String, String> setting : res)
			System.out.print(setting.toString() + " ");
		return res;
	}
	
	public static final String train (String dataType, String lexiconName, String path) {
		Tuple2<String, String>[] settings;
		try {
			settings = loadSettings(SPARK_CONF);
		} catch (IOException e1) {
			e1.printStackTrace();
			return "Unable to load spark configuration!\n" + e1.getMessage();
		}
		
		String master = Arrays.stream(settings)
							.filter((s) -> s._1.equals("master"))
							.findFirst()
							.map(Tuple2::_2)
							.orElse("local[*]");
		Client client = Spark.newClient(master, settings);
		
		Lexicon lexicon;
		try {
			switch (dataType) {
			case "word"  :
			case "words" : lexicon = client.openWordLexicon(lexiconName); break;
			case "topic" :
			case "topics": lexicon = client.openTopicLexicon(lexiconName); break;
			
			default      : return "Not an applicable lexicon data type: " + dataType;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return e.getMessage();
		}
		
		return lexicon.uiTrain(path, client);
	}
	
	public static final String stringToLong(String str) {
		byte[] bytes = Bytes.toBytes(str);
		long lng = Bytes.toLong(bytes);
		return Long.toString(lng);
	}
}
