package rivet.core.arraylabels;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleUnaryOperator;
import java.util.function.IntConsumer;
import java.util.function.IntUnaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;

import scala.Tuple2;
import testing.Log;

public class RIV {
	private static final Log log = new Log("test/rivOutput.txt");
	//Values
	private Point[] points;
	private final int size;
	
	//Constructors
	public RIV(final RIV riv) {
		this.points = riv.points;
		this.size = riv.size;
	}
	public RIV(final int size) {this.points = null; this.size = size;}
	public RIV(final Point[] points, final int size) { this.points = points; this.size = size; }
	public RIV(final int[] keys, final double[] vals, final int size) {
		this.size = size;
		int l = keys.length;
		if (l != vals.length) throw new IndexOutOfBoundsException("Different quantity keys than values!");
		Point[] pts = new Point[l];
		for (int i = 0; i < l; i++)
			pts[i] = Point.pt(keys[i], vals[i]);
		Arrays.sort(pts, Point::compare);
		this.points = pts;
	}
	
	//Methods
	public int size() {return this.size;}
	
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (Point p : this.points) 
			s.append(p.toString() + " ");
		return s.append(this.size).toString();
	}
	
	public Stream<Point> stream() {return Arrays.stream(this.points); }
	public IntStream keyStream() {return this.stream().mapToInt(Point::left); }
	public DoubleStream valStream() {return this.stream().mapToDouble(Point::right); }
	public int[] keys() { return this.keyStream().toArray(); }
	public double[] vals() { return this.valStream().toArray(); }
	
	private void assertValidIndex (int index) {		
		if (index < 0 || index >= this.size) 
			throw new IndexOutOfBoundsException(
					String.format("Requested index is < 0 or > %d: %d", 
							this.size, index));
	}
	
	private int binarySearch(Point pt) {
		return Arrays.binarySearch(this.points, pt, Point::compare);
	}
	private int binarySearch(int index) {
		return this.binarySearch(Point.partial(index)); 
	}
	
	public boolean contains(int index) {
		return ArrayUtils.contains(this.keys(), index);
	}
	
	public Point getPoint(int index) {
		this.assertValidIndex(index);
		int i = this.binarySearch(index);
		return (i < 0)
				? Point.partial(index)
						: this.points[i];
	}
	public double get(int index) {
		return this.getPoint(index).right();
	}
	
	private RIV validSet(int i, Point pt) {
		if (this.points[i].equals(pt))
			this.points[i] = pt;
		else
			ArrayUtils.add(this.points, i, pt);
		return this;
	}
	private RIV validSet(Point pt) {
		final int i = this.binarySearch(pt);
		return this.validSet(
				(i > 0) ? i : ~i,
				pt);
	}
	public RIV set(Point pt) {
		this.assertValidIndex(pt.left());
		return this.validSet(pt);
	}
	public RIV set(int index, double value) { return this.set(Point.pt(index, value)); }
	
	public Point addPoint(Point pt) {
		this.assertValidIndex(pt.left());
		return this.getPoint(pt.left()).add(pt);
	}
	public RIV add(Point pt) {
		return this.addPoint(pt)
				.engage(this::validSet);
	}
	public Point subtractPoint(Point pt) {
		this.assertValidIndex(pt.left());
		return this.getPoint(pt.left()).subtract(pt);
	}
	public RIV subtract(Point pt) {
		return this.subtractPoint(pt)
				.engage(this::validSet);
	}
	
	public RIV forEach(Consumer<Point> c) { 
		this.stream().forEach(c); 
		return this; 
	}
	public RIV forEachIndex(IntConsumer c) {
		this.keyStream().forEach(c);
		return this;
	}
	public RIV forEachValue(DoubleConsumer c) {
		this.valStream().forEach(c);
		return this;
	}
	public RIV map(UnaryOperator<Point> o) {
		Point[] pts = (Point[]) this.stream().map(o).toArray();
		return new RIV(pts, this.size);
	}
	public RIV mapKeys(IntUnaryOperator o) {
		return new RIV(
				this.keyStream().map(o).sorted().toArray(),
				this.vals(),
				this.size);
	}
	public RIV mapVals(DoubleUnaryOperator o) {
		return new RIV(
				this.keys(),
				this.valStream().map(o).sorted().toArray(),
				this.size);
	}
	
	public RIV add(RIV riv) {
		riv.forEach(this::add);
		return this;
	}
	public RIV subtract(RIV riv) {
		riv.forEach(this::subtract);
		return this;
	}
	
	public RIV divideBy(double num) {
		return new RIV(
				this.keys(),
				this.valStream()
					.map((v) -> v / num)
					.toArray(),
				this.size);
	}
	
	private static int[] permuteKeys (int[] keys, int[] permutation, int times) {
		for (int i = 0; i < times; i++)
			for (int c = 0; c < keys.length; c++)
				keys[c] = permutation[keys[c]];
		return keys;
	}
	
	public RIV permute (Tuple2<int[], int[]> permutations, int times) {
		if (times == 0) return this;
		int[] keys = this.keys();
		int[] newKeys =  (times > 0)
				? permuteKeys(keys, permutations._1, times)
						: permuteKeys(keys, permutations._2, times);
		return new RIV(
				newKeys,
				this.vals(),
				this.size);
	}
	
	public Double magnitude() {
		return Math.sqrt(
				this.valStream()
				.map((v) -> v * v)
				.sum());
	}
	
	public RIV normalize() {
		return this.divideBy(this.magnitude());
	}
	
	
	//Static methods
	public static RIV fromString(String rivString) {
		String[] r = rivString.split(" ");
		int l = r.length - 1;
		int size = Integer.parseInt(r[l + 1]);
		Point[] pts = new Point[l];
		for (int i = 0; i < l; i++)
			pts[i] = Point.fromString(r[i]);
		return new RIV(pts, size);
	}
}
