package testing;


import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;

import rivet.util.Util;

@Deprecated
public class Log implements Closeable {
	private PrintStream stream;
	
	public Log (String path) {
		Path dest = Paths.get(path);
		System.out.println("Log created at " + dest.toString());
		try {
		Files.deleteIfExists(dest);
		Files.createFile(dest);
		this.stream = new PrintStream(dest.toString());		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void log (String string) {
		this.stream.format("%s: %s%n", Instant.now().toString(), string);
	}
	
	public void log (Object obj) {
		log(String.valueOf(obj));
	}
		
	public void log (String fmt, Object...args) {
		log(String.format(fmt, args));
	}
	
	public <I extends Number, N extends Number> void logTimeEntry (Instant start, I index, N count) {
		Instant n = Instant.now();
		double c = count.doubleValue();
		double i = index.doubleValue() + 1;
		Duration et = Duration.between(start, n);
		double r = (c - i) * (et.toNanos() / i);
		Duration rt = Duration.ofNanos((long)r);
		log("Item %4.0f out of %4.0f: %.2f%% complete.            %s elapsed, estimate %s remaining",
			i, c, (i/ c) * 100, Util.parseTimeString(et.toString()), Util.parseTimeString(rt.toString()));
	}
	
	public void logEmpty() {stream.print("");}
	public <T> void logArray(T[] arr) {
		StringBuilder s = new StringBuilder();
		for (T t : arr)
			s.append(t.toString() + " ");
		log(s.toString());
	}

	@Override
	public void close() throws IOException {
		this.stream.close();
	}
}
