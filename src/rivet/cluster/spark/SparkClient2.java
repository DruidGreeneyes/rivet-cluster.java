package rivet.cluster.spark;

import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

import org.apache.spark.launcher.SparkAppHandle;

public class SparkClient2 {
	
	public SparkClient2() {
		
	}
	
	public static void test () throws IOException {
		SparkAppHandle.Listener listener = new SparkAppHandle.Listener() {
			
			@Override
			public void stateChanged(SparkAppHandle handle) {
				// TODO Auto-generated method stub
			}
			
			@Override
			public void infoChanged(SparkAppHandle handle) {
				// TODO Auto-generated method stub
			}
		};
		SparkAppHandle l = new SparkLauncher()
				.setMaster("local[2]")
				.setConf(SparkLauncher.DRIVER_MEMORY, "4g")
				.startApplication(listener);
	}
}