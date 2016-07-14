package rivet.cluster.spark;

import static rivet.cluster.spark.Lexica.sfn;
import static rivet.cluster.util.Util.setting;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

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

import rivet.cluster.hbase.HBase;
import rivet.cluster.util.Util;
import rivet.core.labels.MapRIV;
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

    public static MapRIV getInd(final Integer size, final Integer k,
            final String key) {
        return MapRIV.generateLabel(size, k, key);
    }

    // Static and random utility stuff
    private static MapRIV getMeanVector(final JavaPairRDD<String, Row> lexicon,
            final int size) {
        final Long count = lexicon.count();
        return lexicon.values().map(sfn(pointGetter(LEX_COLUMN)))
                .map(MapRIV::fromString).map(MapRIV::normalize)
                .fold(new MapRIV(size), (i, r) -> i.destructiveAdd(r))
                .divide(count);
    }

    public static MapRIV getMeanVector(final Lexicon lexicon) {
        return getMeanVector(lexicon.rdd, lexicon.size);
    }

    public static String getPoint(final Row row, final String columnKey) {
        return pointGetter(columnKey).apply(row);
    }

    public static Function<Row, String> pointGetter(final String columnKey) {
        return (row) -> row.get(columnKey);
    }

    public final String name;

    protected Integer size;

    protected Integer k;

    protected JavaPairRDD<String, Row> rdd;

    protected JavaSparkContext jsc;

    // constructor
    public Lexicon(final JavaSparkContext jsc, final String hbaseTableName)
            throws TableNotFoundException, IOException {
        if (!HBase.tableExists(hbaseTableName))
            throw new TableNotFoundException(
                    "Table not found and not created: " + hbaseTableName);
        this.jsc = jsc;
        rdd = jsc.newAPIHadoopRDD(HBase.newConf(hbaseTableName),
                TableInputFormat.class, ImmutableBytesWritable.class,
                Result.class).mapToPair(Spark::prepareLexiconEntry);
        name = hbaseTableName;
        size = getMetadata(SIZE_COLUMN).orElse(DEFAULT_SIZE);
        k = getMetadata(K_COLUMN).orElse(DEFAULT_K);
    }

    //// Method signatures
    public Lexicon clear() {
        rdd = jsc.parallelizePairs(new ArrayList<Tuple2<String, Row>>());
        return this;
    }

    public long count() {
        return rdd.count();
    }

    public Optional<Row> get(final String key) {
        return Util.getOpt(rdd.lookup(key), 0);
    }

    public MapRIV getInd(final String key) {
        return getInd(size, k, key);
    }

    public Integer getK() {
        return k;
    }

    public Optional<MapRIV> getLex(final String key) {
        return get(key).map(pointGetter(LEX_COLUMN)).map(MapRIV::fromString);
    }

    public MapRIV getLexOrError(final String key) {
        return getLex(key).orElseThrow(
                () -> new IllegalArgumentException("Lexicon " + name
                        + "contains no data for the supplied key: " + key));
    }

    public MapRIV getMeanVector() {
        return getMeanVector(rdd, size);
    }

    public Optional<Integer> getMetadata(final String item) {
        return this.getPoint(META_ROW, item).map(Integer::parseInt);
    }

    public MapRIV getOrMakeLex(final String key) {
        return getLex(key).orElse(this.getInd(key));
    }

    public Optional<String> getPoint(final String key, final String columnKey) {
        return get(key).map(pointGetter(columnKey));
    }

    public Integer getSize() {
        return size;
    }

    public Lexicon setK(final Integer k) {
        this.k = k;
        return this;
    }

    public Lexicon setSize(final Integer size) {
        this.size = size;
        return this;
    }

    // Abstracts
    public abstract String uiTrain(String path);

    public Lexicon write() throws IOException {
        List<String> hbaseQuorum;
        try {
            hbaseQuorum = Files
                    .readAllLines(Paths.get("conf/hbase.zookeeper.quorum.conf"))
                    .stream().filter((line) -> !line.trim().startsWith("#"))
                    .collect(Collectors.toList());
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(
                    "Unable to load hbase.zookeeper.quorum configuration!\n"
                            + e.getMessage());
        }
        final Configuration conf = HBase.newConf(
                setting(TableOutputFormat.OUTPUT_TABLE, name),
                setting("hbase.zookeeper.quorum", hbaseQuorum.get(0)));
        final Job job = Job.getInstance(conf);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        rdd.mapToPair(Spark::prepareEntryForStorage)
                .saveAsNewAPIHadoopDataset(job.getConfiguration());
        return this;
    }
}
