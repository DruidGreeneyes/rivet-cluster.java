package rivet.cluster.spark;

import static rivet.cluster.spark.Lexica.sfn;
import static rivet.util.Util.setting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import rivet.core.arraylabels.Labels;
import rivet.core.arraylabels.RIV;
import rivet.persistence.hbase.HBase;
import rivet.util.Util;
import scala.Tuple2;

public abstract class Lexicon {
	
	public static final String LEX_COLUMN = "lex";
	
	public static final String META_ROW = "metadata";

	public static final String SIZE_COLUMN = "size";
	public static final String K_COLUMN = "k";
	public static final String CR_COLUMN = "cr";
	
	public static final Integer DEFAULT_SIZE = 16000;
	public static final Integer DEFAULT_K = 48;
	public static final Integer DEFAULT_CR = 3;
	
	protected final String name;
	protected Integer size;
	protected Integer k;
	
	protected JavaPairRDD<String, Row> rdd;
	protected JavaSparkContext jsc;
	//constructor
	public Lexicon (final JavaSparkContext jsc, final String hbaseTableName) throws TableNotFoundException, IOException {
		if (!HBase.tableExists(hbaseTableName)) 
			throw new TableNotFoundException("Table not found and not created: " + hbaseTableName);
		this.jsc = jsc;
		this.rdd = jsc.newAPIHadoopRDD(
				HBase.newConf(hbaseTableName),
				TableInputFormat.class,
				ImmutableBytesWritable.class,
				Result.class)
			.mapToPair(Spark::prepareEntryForWork);
		this.name = hbaseTableName;
		this.size = this.getMetadata(SIZE_COLUMN).orElse(DEFAULT_SIZE);
		this.k = this.getMetadata(K_COLUMN).orElse(DEFAULT_K);
	}
	
	////Method signatures
	public Lexicon clear() {
		this.rdd = jsc.parallelizePairs(new ArrayList<Tuple2<String, Row>>());
		return this;
	}
	
	public long count() {return this.rdd.count();}
	
	public Lexicon write() throws IOException {
		Configuration conf = HBase.newConf(
				setting(TableOutputFormat.OUTPUT_TABLE, this.name),
				setting("hbase.zookeeper.quorum", "localhost"));
		Job job = Job.getInstance(conf);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);
		this.rdd
			.mapToPair(Spark::prepareEntryForStorage)
			.saveAsNewAPIHadoopDataset(job.getConfiguration());
		return this;
	}
	
	public Integer getSize () {return this.size;}
	public Integer getK () {return this.k;}
	
	public Lexicon setSize (final Integer size) {this.size = size; return this;}
	public Lexicon setK (final Integer k) {this.k = k; return this;}
	
	public Optional<Row> get (String key) {
		return Util.getOpt(this.rdd.lookup(key), 0);
	}
	
	public static Function<Row, String> pointGetter(String columnKey) {
		return (row) -> row.get(columnKey);
	}
	
	public static String getPoint (Row row, String columnKey) {
		return pointGetter(columnKey).apply(row);
	}
	
	public Optional<String> getPoint (String key, String columnKey) {
		return this.get(key).map(pointGetter(columnKey));
	}
	
	public Optional<Integer> getMetadata (String item) {
		return this.getPoint(META_ROW, item).map(Integer::parseInt);
	}
	
	public static RIV getInd (final Integer size, final Integer k, final String key) {
		return Labels.generateLabel(size, k, key);
	}
	public RIV getInd (final String key) {
		return getInd(this.size, this.k, key);
	}
	
	public Optional<RIV> getLex (final String key) {
		return this.get(key)
				.map(pointGetter(LEX_COLUMN))
				.map(RIV::fromString);
	}
	
	public RIV getOrMakeLex (final String key) {
		return this.getLex(key)
				.orElse(this.getInd(key));
	}
	
	public RIV getMeanVector() {
		return getMeanVector(this.rdd);
	}
	
	//Static and random utility stuff
	public static RIV getMeanVector(JavaPairRDD<String, Row> lexicon) {
		final Long count = lexicon.count();
		return lexicon
				.values()
				.map(sfn(pointGetter(LEX_COLUMN)))
				.map(RIV::fromString)
				.map(RIV::normalize)
				.reduce(Labels::addLabels)
				.divideBy(count);
	}
	public static RIV getMeanVector(Lexicon lexicon) { return getMeanVector(lexicon.rdd); }
	
	//Abstracts
	public abstract String uiTrain(String path);
}
