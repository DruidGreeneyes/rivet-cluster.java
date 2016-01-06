package rivet.cluster.spark;

import java.io.Closeable;
import java.io.IOException;
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
import rivet.persistence.HBase;
import rivet.persistence.hbase.Delete;

public class SparkTable implements Closeable {
	private Connection conn;
	
	public String name;
	public TableName tableName;
	public String rowsKey;
	public Admin admin;
	public Table table;
	public JavaPairRDD<String, Row> rdd;
	
	public static String META = "metadata";
	
	
	//Constructor & Friends
	public SparkTable(String rowsKey, JavaPairRDD<String, Row> rdd) throws IOException {
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
	
	public void addColumnFamily (String familyKey) throws IOException {
		this.admin.addColumn(
				this.tableName,
				new HColumnDescriptor(familyKey));
	}
	
	public void ensureFamilyExists (String familyKey) throws IOException {
		if (!this.table.getTableDescriptor().hasFamily(HBase.stringToBytes(familyKey)))
			this.addColumnFamily(familyKey);
	}
	
	public Optional<Row> get(String rowKey) {
		List<Row> rows = this.rdd.lookup(rowKey);
		return (rows.isEmpty() || rows.get(0).isEmpty())
				? Optional.empty()
						: Optional.of(rows.get(0));
	}
	
	public Optional<String> getPoint(String rowKey, String columnKey) {
		return this.get(rowKey)
				.map((cells) -> cells.get(columnKey));
	}
	
	public Optional<Integer> getMetadata(String item) {
		return this.getPoint(META, item).map(Integer::parseInt);
	}
	
	public Optional<Row> put (String rowKey, Row row) throws IOException {
		Row r = new Row(row);
		r.put(this.rowsKey, rowKey);
		this.table.put(new Put(r, this.rowsKey));
		return this.get(rowKey);
	}
	
	public Optional<String> setPoint(String rowKey, String columnKey, String value) throws IOException {
		Row row = this.get(rowKey).orElse(new Row());
		row.put(columnKey, value);
		return this.put(rowKey, row).map((cells) -> cells.get(columnKey));
	}
	
	public Optional<Integer> setMetadata(String item, Integer value) throws IOException {
		return this.setPoint(META, item, value.toString()).map(Integer::parseInt);
	}
	
	public Optional<Row> delete(String rowKey) throws IOException {
		this.table.delete(new Delete(rowKey));
		return this.get(rowKey);
	}
	
	public Optional<String> deletePoint(String rowKey, String familyKey, String columnKey) throws IOException {
		this.table.delete(new Delete(rowKey, familyKey, columnKey));
		return this.getPoint(rowKey, columnKey);
	}
	
	public boolean clear () throws IOException {
		this.rdd.keys().collect().forEach((rowKey) -> {
			try {
				this.delete(rowKey);
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		return this.rdd.count() == 0;
	}
}
