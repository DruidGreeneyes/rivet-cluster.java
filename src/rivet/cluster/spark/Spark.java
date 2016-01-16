package rivet.cluster.spark;

import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;

import rivet.core.hashlabels.HashLabels;
import rivet.core.hashlabels.RIV;
import rivet.persistence.hbase.HBase;
import rivet.util.Util;
import scala.Tuple2;

public class Spark {	
	public static final Tuple2<Optional<?>, Optional<?>> EMPTY_ENTRY = 
			Tuple2.apply(Optional.empty(), Optional.empty());
	
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
		return Tuple2.apply(key, val);
	}
	
	public static Tuple2<ImmutableBytesWritable, Put> prepareEntryForStorage(
			Tuple2<String, Row> entry) {
		ImmutableBytesWritable key = HBase.stringToIBW(entry._1);
		Put val = HBase.newPut(key, entry._2);
		return Tuple2.apply(key, val);
	}
	
	public static <R> Tuple2<ImmutableBytesWritable, R> keyToIBW(Tuple2<String, R> entry) {
		return Tuple2.apply(
				new ImmutableBytesWritable(Bytes.toBytes(entry._1)),
				entry._2);
	}
	
	public static Row mergeJoinEntry(Tuple2<Optional<Row>, Optional<RIV>> entry) {
		if (entry._1.isPresent() && !entry._2.isPresent())
			return entry._1.get();
		
		Row row = entry._1.orElse(new Row());
		Optional<RIV> oldRIV = Optional.ofNullable(row.get("lex")).map(RIV::new);
		RIV newRIV = Util.mergeOptions(
					entry._2,
					oldRIV,
					HashLabels::addLabels)
				.orElseThrow(IndexOutOfBoundsException::new);	
		row.put("lex", newRIV.toString());
		return row;
	}
	
	
}
