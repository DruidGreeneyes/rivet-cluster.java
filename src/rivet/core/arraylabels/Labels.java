package rivet.core.arraylabels;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang.ArrayUtils;

import rivet.util.Util;
import scala.Tuple2;
import testing.Log;

public class Labels {
	private static final Log log = new Log("test/labelsOutput.txt");
	
	private Labels(){}
	
	public static IntStream getMatchingKeyStream (final RIV labelA, final RIV labelB) {
		return labelA.keyStream().filter(labelB::contains);
	}
	
	public static Stream<Tuple2<Double, Double>> getMatchingValStream (final RIV labelA, final RIV labelB) {
		return labelA.keyStream().filter(labelB::contains).mapToObj((i) -> Tuple2.apply(labelA.get(i), labelB.get(i)));
	}
	
	public static double dotProduct (final RIV labelA, final RIV labelB) {
		return getMatchingValStream(labelA, labelB)
				.mapToDouble((vs) -> vs._1 * vs._2)
				.sum();
	}
	
	public static RIV addLabels(final RIV labelA, final RIV labelB) { return labelA.add(labelB); }
	
	public static double[] makeVals (final int count, long seed) {
		double[] l = new double[count];
		for (int i = 0; i < count; i += 2) l[i] = 1;
		for (int i = 1; i < count; i += 2) l[i] = -1;
		return Util.shuffleDoubleArray(l, seed);
	}
	
	public static int[] makeIndices(final int size, final int count, final long seed) {
		return Util.randInts(size, count, seed).toArray();
	}
	
	public static RIV generateLabel (final int size, final int k, final String word) {
		final long seed = word.hashCode();
		final int j = (k % 2 == 0) ? k : k + 1;
		return new RIV(
				makeIndices(size, j, seed),
				makeVals(j, seed),
				size);
	}
	
	public static Tuple2<int[], int[]> generatePermutations (int size) {
		int[] permutation = Util.randInts(size, size, 0L)
									.toArray();
		int[] inverse = new int[size];
		for (int i = 0; i < size; i++)
			inverse[i] = ArrayUtils.indexOf(permutation, i);
		return Tuple2.apply(permutation, inverse);
	}
}
