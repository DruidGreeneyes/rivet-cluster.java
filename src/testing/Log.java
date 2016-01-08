package testing;


import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Clock;

public class Log implements Closeable {
	private PrintStream stream;
	
	public Log (String path) throws FileNotFoundException {
		stream = new PrintStream(path);
	}
	
	public void log (String string) {
		stream.println(Clock.systemUTC().instant().toString() + ": " + string);
	}

	@Override
	public void close() throws IOException {
		stream.close();
	}
}
