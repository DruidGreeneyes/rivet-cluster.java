package rivet.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class ReadTable extends ArrayList<Character> {
	private static final long serialVersionUID = 2175975977751763383L;
	/**
	 * 
	 */
	private final Map<String, Consumer<ReadTable>> actions;
	private final InputStream input;
	private final Stream<Integer> stream;
	
	public ReadTable (InputStream input) {
		super();
		this.actions = new HashMap<>();
		this.input = input;
		this.stream = Stream.generate(this::read)
								.peek(this::act);
	}
	
	public Integer read() {
		int r = -1;
		try {
			r = input.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return r;
	}
	
	public void act (Integer i) {
		if (i >= 0) {
			this.add((char) i.intValue());
			Consumer<ReadTable> action = this.getAction(this.table());
			if (action != null) {
				action.accept(this);
			}
		}
	}
	
	public Boolean setAction (String name, Consumer<ReadTable> action) {
		Boolean present = this.actions.containsKey(name);
		this.actions.put(name, action);
		return present;
	}
	
	public Consumer<ReadTable> getAction (String name) {
		return this.actions.get(name);
	}

	public String table () {
		StringBuilder sb = new StringBuilder(this.size());
		this.forEach(sb::append);
		return sb.toString();
	}
}
