package rivet.util;

public class Counter {
	private Long c;
	public Counter() {this.c = 0L;}
	public Counter(Number init) {this.c = init.longValue();}
	
	public Long get() {return this.c;}
	public Long inc() {this.c++; return this.c;}
	public Long dec() {this.c--; return this.c;}
	public Long set(Number val) {this.c = val.longValue(); return this.c;}
	public Long zero() {this.c = 0L; return this.c;}
}
