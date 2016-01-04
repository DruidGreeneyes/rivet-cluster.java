package rivet.persistence.hbase;

import java.io.Serializable;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.util.HeapMemorySizeUtil;
import org.apache.hadoop.hbase.util.VersionInfo;

@SuppressWarnings("deprecation")
public class HBaseConf extends HBaseConfiguration implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8125725779188393081L;

	private static void checkDefaultsVersion(HadoopConf conf) {
		if (conf.getBoolean("hbase.defaults.for.version.skip", Boolean.FALSE)) return;
	    String defaultsVersion = conf.get("hbase.defaults.for.version");
	    String thisVersion = VersionInfo.getVersion();
	    if (!thisVersion.equals(defaultsVersion)) {
	      throw new RuntimeException(
	        "hbase-default.xml file seems to be for an older version of HBase (" +
	        defaultsVersion + "), this version is " + thisVersion);
	    }
	}
	
	public static HadoopConf addHBaseResources (HadoopConf conf) {
		conf.addResource("hbase-default.xml");
		conf.addResource("hbase-site.xml");
		checkDefaultsVersion(conf);
		HeapMemorySizeUtil.checkForClusterFreeMemoryLimit(conf);
		return conf;
	}

	public static HadoopConf create() {
		HadoopConf conf = new HadoopConf();
		conf.setClassLoader(HBaseConf.class.getClassLoader());
		return addHBaseResources(conf);
	}
}
