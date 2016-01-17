package rivet.core.arraylabels;

import java.util.function.BiFunction;
import java.util.function.Function;

public class Point implements Comparable<Point> {
	//Values
	private final int left;
	private final double right;
	
	//Constructors
	private Point() { this.left = 0; this.right = 0; }
	private Point(int left, double right) { this.left = left; this.right = right; }
	private Point(int left) {this.left = left; this.right = 0;}
	private Point(double right) {this.left = 0; this.right = right;}
	
	//Core Methods
	public int left() { return this.left; }
	public double right() { return this.right; }
	public String toString() { return String.format("%d/%d", left, right);}
	@Override
	public int compareTo(Point p) {	return Integer.compare(this.left, p.left); }
	public boolean equals(Point p) { return this.compareTo(p) == 0; }
	private void assertMatch(Point p) {
		if (this.compareTo(p) != 0)
			throw new IndexOutOfBoundsException(
					String.format("Point indices do not match! %s != %s",
							this.toString(), p.toString()));
	}
	public Point add(double v) { return pt(this.left, this.right + v); }
	public Point add(Point p) {
		this.assertMatch(p);
		return this.add(p.right);
	}
	public Point subtract(double v) { return this.add(-v); }
	public Point subtract(Point p) { return this.add(-p.right); }
	
	//Convenience Methods
	public <T> T engage (Function<Point, T> fun) {return fun.apply(this);}
	public <T, R> R engage (BiFunction<Point, T, R> fun, T thing) { return fun.apply(this, thing); }
	
	
	//Static Methods
	public static Point zero() { return new Point(); }
	public static Point partial(int left) { return new Point(left); }
	public static Point partial(double right) { return new Point(right); }
	public static Point pt(int left, double right) { return new Point(left, right); }
	public static Point fromString(String ptString) {
		String[] pt = ptString.split("\\|");
		if (pt.length != 2) throw new IndexOutOfBoundsException("Wrong number of partitions: " + ptString);
		return pt(
				Integer.parseInt(pt[0]),
				Double.parseDouble(pt[1]));
	}
	public static int compare (Point a, Point b) { return a.compareTo(b); }
	
}
