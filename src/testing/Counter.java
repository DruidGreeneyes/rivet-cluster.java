package testing;

public class Counter {
	private long c;
	public Counter() {this.c = 0;}
	
	public long get() {return this.c;}
	public long inc() {this.c++; return this.c;}
	public long dec() {this.c--; return this.c;}
}
