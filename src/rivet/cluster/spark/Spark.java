package rivet.cluster.spark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import rivet.core.arraylabels.Labels;
import rivet.core.arraylabels.RIV;
import rivet.persistence.hbase.HBase;
import rivet.util.Util;
import scala.Tuple2;

public class Spark {
	
	public static SparkConf newSparkConf() {return new SparkConf().setAppName("Rivet");}
	public static SparkConf newSparkConf(final String master, List<Setting> settings) {
		final SparkConf sparkConf = newSparkConf()
				.setMaster(master);
		settings.forEach((entry) -> sparkConf.set(entry._1, entry._2));
		return sparkConf;
	}
	public static SparkConf newSparkConf(final String master, Setting...settings) {return newSparkConf(master, Arrays.asList(settings));}
	public static SparkConf newSparkConf(final List<Setting> settings) {
		Optional<Setting> hasMaster = settings.stream()
										.filter((s) -> s._1.equals("master"))
										.findFirst();
		String master = (hasMaster.isPresent())
				? settings.remove(settings.indexOf(hasMaster.get()))._2
						: "local[*]";
		return newSparkConf(master, settings);
	}
	public static SparkConf newSparkConf(Setting...settings) {return newSparkConf(Arrays.asList(settings));}
	
	public static WordLexicon openWordLexicon (JavaSparkContext jsc, String name) throws IOException {
		return new WordLexicon(jsc, name);
	}
	
	public static TopicLexicon openTopicLexicon (WordLexicon wordLexicon, String name) throws IOException {
		return new TopicLexicon(wordLexicon, name);
	}
	
	public static final Tuple2<Optional<?>, Optional<?>> EMPTY_ENTRY = 
			new Tuple2<>(Optional.empty(), Optional.empty());
	
	public static SparkConf newSparkConf(String master, Map<String, String> settings) {
		SparkConf res = new SparkConf().setAppName("Rivet")
									.setMaster(master);
		settings.forEach(res::set);
		return res;
	}
	
	public static Tuple2<String, Row> prepareEntryForWork(
			Tuple2<ImmutableBytesWritable, Result> entry) {
		String key = HBase.ibwToString(entry._1);
		Row val = new Row(entry._2);
		return new Tuple2<>(key, val);
	}
	
	public static Tuple2<ImmutableBytesWritable, Put> prepareEntryForStorage(
			Tuple2<String, Row> entry) {
		ImmutableBytesWritable key = HBase.stringToIBW(entry._1);
		Put val = HBase.newPut(key, entry._2);
		return new Tuple2<>(key, val);
	}
	
	public static <R> Tuple2<ImmutableBytesWritable, R> keyToIBW(Tuple2<String, R> entry) {
		return new Tuple2<>(
				new ImmutableBytesWritable(Bytes.toBytes(entry._1)),
				entry._2);
	}
	
	public static Row mergeJoinEntry(Tuple2<Optional<Row>, Optional<RIV>> entry) {
		if (entry._1.isPresent() && !entry._2.isPresent())
			return entry._1.get();
		
		Row row = entry._1.orElse(new Row());
		Optional<RIV> oldRIV = Optional.ofNullable(row.get("lex")).map(RIV::fromString);
		RIV newRIV = Util.mergeOptions(
					entry._2,
					oldRIV,
					Labels::addLabels)
				.orElseThrow(IndexOutOfBoundsException::new);	
		row.put("lex", newRIV.toString());
		return row;
	}
}
