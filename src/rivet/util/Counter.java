package rivet.util;

public class Counter {
	private long c;
	public Counter() {this.c = 0L;}
	public Counter(Number init) {this.c = init.longValue();}
	
	public long get() {return this.c;}
	public long inc() {this.c++; return this.c;}
	public long dec() {this.c--; return this.c;}
	public long set(Number val) {this.c = val.longValue(); return this.c;}
	public Counter zero() {this.c = 0L; return this;}
}
