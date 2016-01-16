package rivet.core.hashlabels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import rivet.util.Counter;
import rivet.util.Util;
import scala.Tuple2;
import testing.Log;

public final class HashLabels {
	@SuppressWarnings("unused")
	private static final Log log = new Log("test/hashLabelsOutput.txt");
	
	private HashLabels(){}
	
	public static Double dotProduct (final RIV labelA, final RIV labelB) {
		return Util.getMatchingKeys(labelA, labelB)
				.parallelStream()
				.mapToDouble((x) -> labelA.get(x) * labelB.get(x))
				.sum();
	}
	
	public static Double similarity (final RIV labelA, final RIV labelB) {
		final RIV lA = labelA.normalize();
		final RIV lB = labelB.normalize();
		final Double mag = lA.magnitude() * lB.magnitude();
		return (mag == 0)
				? 0
				: dotProduct(lA, lB) / mag;
	}
	
	public static RIV addLabels (final RIV labelA, final RIV labelB) {
		final RIV result = new RIV(labelA);
		labelB.entrySet().forEach(result::mergePlus);
		return result;
	}
	
	public static List<Double> makeKs (final Integer k, final Long seed) {
		final List<Double> l = new ArrayList<>();
		for (Integer i = 0; i < k; i++) 
			l.add((i < k / 2) 
					? 1.0 
					: -1.0);
		return Util.shuffleList(l, seed);
	}
	
	public static SortedSet<Integer> makeIndices (final Integer size, final Integer k, final Long seed) {
		return Util.randInts(size, k, seed)
				.boxed()
				.collect((Collectors.toCollection(TreeSet::new)));
	}
	
	public static Long makeSeed (final String word) {
		return word.chars()
				.boxed()
				.mapToLong((x) -> x.longValue() * (10 ^ (word.indexOf(x))))
				.sum();
	}
	
	public static Integer makeEven (final Integer x) { 
		return (x % 2 == 0) ? x : x + 1; 
		}
	
	public static RIV generateLabel (final Integer size, final Integer k, final String word) {
		final Long seed = makeSeed(word);
		final Integer j = makeEven(k);
		return new RIV(
				makeIndices(size, j, seed),
				makeKs(j, seed),
				size,
				j);
	}
	
	public static Tuple2<int[], int[]> generatePermutations (int size) {
		int[] base = Util.range(size).toArray();
		int[] permutation = Util.randInts(0, size, 0L)
									.distinct()
									.limit(size)
									.toArray();
		int[] inverse = base.clone();
		for (int i : inverse)
			i = base[permutation[i]];
		return Tuple2.apply(permutation, inverse);
	}
}
