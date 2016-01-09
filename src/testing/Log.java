package testing;


import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import rivet.Util;

public class Log implements Closeable {
	private PrintStream stream;
	
	public Log (String path) throws FileNotFoundException {
		stream = new PrintStream(path);
	}
	
	public void log (String string) {
		stream.format("%s: %s%n", Clock.systemUTC().instant().toString(), string);
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
	
	public <N extends Number> void logTimeEntry (Instant start, N index) {
		Instant n = Instant.now();
		long i = index.longValue();
		Duration et = Duration.between(start, n);
		log("Item %d                %s elapsed",
				i, Util.parseTimeString(et.toString()));
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}
}
