package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.api.java.JavaPairRDD;

import rivet.persistence.hbase.Put;
import rivet.util.Counter;
import testing.Log;
import rivet.persistence.HBase;
import rivet.persistence.hbase.Delete;

public class SparkTable implements Closeable {
	private static final Log log = new Log("test/sparkTableOutput.txt");
	private final Connection conn;
	
	public final String name;
	public final TableName tableName;
	public final String rowsKey;
	public final Admin admin;
	public final Table table;
	public final JavaPairRDD<String, Row> rdd;
	
	public static final String META = "metadata";
	
	
	//Constructor & Friends
	public SparkTable(final String rowsKey, final JavaPairRDD<String, Row> rdd) throws IOException {
		this.rowsKey = rowsKey;
		this.rdd = rdd;
		this.name = rdd.name();
		this.tableName = TableName.valueOf(this.name);
		this.conn = ConnectionFactory.createConnection(
				rdd.context().hadoopConfiguration());
		this.admin = this.conn.getAdmin();
		this.table = this.conn.getTable(tableName);
		this.ensureFamilyExists("data");
	}

	//Instance Methods
	@Override
	public void close() throws IOException {
		this.table.close();
		this.admin.close();
		this.conn.close();
	}
	
	public void addColumnFamily (final String familyKey) throws IOException {
		this.admin.addColumn(
				this.tableName,
				new HColumnDescriptor(familyKey));
	}
	
	public void ensureFamilyExists (final String familyKey) throws IOException {
		if (!this.table.getTableDescriptor().hasFamily(HBase.stringToBytes(familyKey)))
			this.addColumnFamily(familyKey);
	}
	
	public Optional<Row> get(final String rowKey) {
		final List<Row> rows = this.rdd.lookup(rowKey);
		return (rows.isEmpty() || rows.get(0).isEmpty())
				? Optional.empty()
						: Optional.of(rows.get(0));
	}
	
	public Optional<String> getPoint(final String rowKey, final String columnKey) {
		return this.get(rowKey)
				.map((cells) -> cells.get(columnKey));
	}
	
	public Optional<Integer> getMetadata(final String item) {
		return this.getPoint(META, item).map(Integer::parseInt);
	}
	
	public Optional<Row> put (final String rowKey, final Row row) throws IOException {
		final Row r = new Row(row);
		r.put(this.rowsKey, rowKey);
		this.table.put(new Put(r, this.rowsKey));
		return this.get(rowKey);
	}
	
	public Optional<String> setPoint(final String rowKey, final String columnKey, final String value) throws IOException {
		final Row row = this.get(rowKey).orElse(new Row());
		row.put(columnKey, value);
		return this.put(rowKey, row).map((cells) -> cells.get(columnKey));
	}
	
	public Optional<Integer> setMetadata(final String item, final Integer value) throws IOException {
		return this.setPoint(META, item, value.toString()).map(Integer::parseInt);
	}
	
	public Optional<Row> delete(final String rowKey) throws IOException {
		this.table.delete(new Delete(rowKey));
		return this.get(rowKey);
	}
	
	public Optional<String> deletePoint(final String rowKey, final String familyKey, final String columnKey) throws IOException {
		this.table.delete(new Delete(rowKey, familyKey, columnKey));
		return this.getPoint(rowKey, columnKey);
	}
	
	public Long count() {
		return this.rdd.count();
	}
	
	public Boolean clear () throws IOException {
		log.log("Clearing database '%s'", this.rdd.name());
		final Counter c = new Counter();
		final Long count = this.rdd.count();
		final Instant t = Instant.now();
		this.rdd.keys().collect().parallelStream().forEach((rowKey) -> {
			log.logTimeEntry(t, c.inc(), count);
			try {
				this.delete(rowKey);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		return this.rdd.count() == 0;
	}
}
