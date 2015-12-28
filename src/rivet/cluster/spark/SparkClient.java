package rivet.cluster.spark;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkClient {
	private SparkConf sc;
	private JavaSparkContext jsc;
	
	public SparkClient () {
		
	}
	
	public static void main (String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		SparkConf sc = new SparkConf().setAppName("HBaseRead").setMaster("local[2]");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		Configuration conf = HBaseConfiguration.create();
		TableName tn = TableName.valueOf("test");
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
	}
}