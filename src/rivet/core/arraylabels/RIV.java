package rivet.core.arraylabels;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
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
	private VectorElement[] points;
	private final int size;
	
	//Constructors
	public RIV(final RIV riv) {
		this.points = riv.points;
		this.size = riv.size;
	}
	public RIV(final int size) {this.points = new VectorElement[0]; this.size = size;}
	public RIV(final VectorElement[] points, final int size) { this.points = points; this.size = size; }
	public RIV(final int[] keys, final double[] vals, final int size) {
		this.size = size;
		int l = keys.length;
		if (l != vals.length) throw new IndexOutOfBoundsException("Different quantity keys than values!");
		VectorElement[] elts = new VectorElement[l];
		for (int i = 0; i < l; i++)
			elts[i] = VectorElement.elt(keys[i], vals[i]);
		Arrays.sort(elts, VectorElement::compare);
		this.points = elts;
	}
	
	//Methods
	public int size() {return this.size;}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (VectorElement p : this.points) 
			s.append(p.toString() + " ");
		return s.append(this.size).toString();
	}
	
	public <T> T engage(Function<RIV, T> fun) {return fun.apply(this);} 
	
	public Stream<VectorElement> stream() {return Arrays.stream(this.points); }
	public IntStream keyStream() {return this.stream().mapToInt(VectorElement::index); }
	public DoubleStream valStream() {return this.stream().mapToDouble(VectorElement::value); }
	public int[] keys() { return this.keyStream().toArray(); }
	public double[] vals() { return this.valStream().toArray(); }
	
	private void assertValidIndex (int index) {		
		if (index < 0 || index >= this.size) 
			throw new IndexOutOfBoundsException(
					String.format("Requested index is < 0 or > %d: %d", 
							this.size, index));
	}
	
	private int binarySearch(VectorElement elt) {
		log.log("RIV.binarySearch(Point) called with " + elt.toString());
		return Arrays.binarySearch(this.points, elt, VectorElement::compare);
	}
	private int binarySearch(int index) {
		log.log("RIV.binarySearch(index) called with " + index);
		return this.binarySearch(VectorElement.partial(index)); 
	}
	
	public boolean contains(int index) {
		return ArrayUtils.contains(this.keys(), index);
	}
	
	public VectorElement getPoint(int index) {
		log.log("RIV.getPoint(index) called with " + index);
		this.assertValidIndex(index);
		int i = this.binarySearch(index);
		return (i < 0)
				? VectorElement.partial(index)
						: this.points[i];
	}
	public double get(int index) {
		return this.getPoint(index).value();
	}
	
	private RIV validSet(int i, VectorElement elt) {
		log.log("RIV.validSet(i, Point) called with " + i + ", " + elt.toString());
		if (this.points.length == 0)
			ArrayUtils.add(this.points, 0, elt);
		else
			if (this.points[i].equals(elt))
				this.points[i] = elt;
			else
				ArrayUtils.add(this.points, i, elt);
		return this;
	}
	private RIV validSet(VectorElement elt) {
		log.log("RIV.validSet(Point) called with " + elt.toString());
		final int i = this.binarySearch(elt);
		return this.validSet(
				(i > 0) ? i : ~i,
				elt);
	}
	public RIV set(VectorElement elt) {
		this.assertValidIndex(elt.index());
		return this.validSet(elt);
	}
	public RIV set(int index, double value) { return this.set(VectorElement.elt(index, value)); }
	
	private VectorElement addPoint(VectorElement elt) {
		log.log("RIV.addPoint(Point) called with " + elt.toString());
		this.assertValidIndex(elt.index());
		return this.getPoint(elt.index()).add(elt);
	}
	public RIV add(VectorElement elt) {
		log.log("RIV.add(Point) called with " + elt.toString());
		return this.addPoint(elt)
				.engage(this::validSet);
	}
	private VectorElement subtractPoint(VectorElement elt) {
		this.assertValidIndex(elt.index());
		return this.getPoint(elt.index()).subtract(elt);
	}
	public RIV subtract(VectorElement elt) {
		return this.subtractPoint(elt)
				.engage(this::validSet);
	}
	
	public RIV forEach(Consumer<VectorElement> c) { 
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
	public RIV map(UnaryOperator<VectorElement> o) {
		VectorElement[] elts = (VectorElement[]) this.stream().map(o).toArray();
		return new RIV(elts, this.size);
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
		return this.removeZeros();
	}
	public RIV subtract(RIV riv) {
		return riv.mapVals(x -> -x)
				.engage(this::add);
	}
	
	public RIV divideBy(double num) {
		return new RIV(
				this.keys(),
				this.valStream()
					.map((v) -> v / num)
					.toArray(),
				this.size);
	}
	
	private RIV removeZeros () {
		this.points = (VectorElement[])
				this.stream()
					.filter((elt) -> !elt.contains(0))
					.toArray();
		return this;
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
		VectorElement[] elts = new VectorElement[l];
		for (int i = 0; i < l; i++)
			elts[i] = VectorElement.fromString(r[i]);
		return new RIV(elts, size);
	}
}
