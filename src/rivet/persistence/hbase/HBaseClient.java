package rivet.persistence.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import rivet.Util;
import rivet.persistence.HBase;
import rivet.persistence.hbase.Get;
import rivet.persistence.hbase.Put;


public class HBaseClient implements Closeable {
	//Vars
	private Connection conn;
	public Admin admin;
	public Table table;
	
	//Constructors	
	public HBaseClient (String tableName) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		this.conn = ConnectionFactory.createConnection(conf);
		this.admin = this.conn.getAdmin();
		Optional<Table> t = this.getTable(tableName);
		if (!t.isPresent()) 
			throw new IOException("Table does not exist: " + tableName);
		this.table = t.get();
	}
	
	public HBaseClient (Configuration conf) throws IOException {
		this.conn = ConnectionFactory.createConnection(conf);
		this.admin = this.conn.getAdmin();
		String tableName = conf.get(TableInputFormat.INPUT_TABLE);
		Optional<Table> t = this.getTable(tableName);
		if (!t.isPresent()) 
			this.makeTable(tableName);
		this.table = t.get();
	}
	
	
	//Methods
	public void close () throws IOException {
		this.table.close();
		this.admin.close();
		this.conn.close();
	}
	
	public void closeConn () throws IOException {
		this.conn.close();
	}
	
	public Result get (String row) throws IOException {
		return this.table.get(new Get(row));
	}
	public Map<String, String> getWord (String word) throws IOException {
		return HBase.resultToStrings(this.get(word));
	}
	
	public List<Result> getBy (int num, Function <Result, Double> keyfn) throws IOException {
		List<Result> res = new ArrayList<>();
		Scan scan = new Scan();
		try (ResultScanner rs = this.table.getScanner(scan)) {
			rs.forEach(res::add);
		}
		return Util.takeBy(num, keyfn, res);
	}
	
	public void put (Table table, Map<String, String> row) throws IOException {
		table.put(new Put(row, "word"));
	}
	
	public void put (Map<String, String> row) throws IOException {
		this.table.put(new Put(row, "word"));
	}
	
	public void setWord (String word, String riv) throws IOException {
		Map<String, String> res = new HashMap<>();
		res.put("word", word);
		res.put("lex", riv);
		this.put(res);
	}
	
	public void delete (String row) throws IOException {
		Delete del = new Delete(HBase.stringToBytes(row));
		del.addFamily(HBase.stringToBytes("data"));
		this.table.delete(del);
	}
	
	
	
	public Table makeTable (TableName tableName) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		this.admin.createTable(htd);
		return this.conn.getTable(tableName);
	}
	
	public Table makeTable (String tableName) throws IOException {
		return this.makeTable(TableName.valueOf(tableName));
	}
	
	public Table makeTable (String tableName, String[] columns) throws IOException {
		TableName tn = TableName.valueOf(tableName);
		this.makeTable(tn);
		for (String c : columns)
			this.admin.addColumn(tn, new HColumnDescriptor(c));
		return this.conn.getTable(tn);
	}
	
	public void addColumnFamily (String tableName, String column) throws IOException {
		this.admin.addColumn(
				TableName.valueOf(tableName),
				new HColumnDescriptor(column));
	}
	
	private Optional<Table> getTable (String tableName) throws IOException {
		TableName tn = TableName.valueOf(tableName);
		if (this.admin.tableExists(tn)) 
			return Optional.of(this.conn.getTable(tn));
		else 
			return Optional.empty();
	}
}
