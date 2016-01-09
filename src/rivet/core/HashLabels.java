package rivet.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import rivet.Util;
import rivet.core.RIV;

public final class HashLabels {
	private HashLabels(){}
	
	public static int dotProduct (RIV labelA, RIV labelB) {
		return Util.getMatchingKeys(labelA, labelB)
				.parallelStream()
				.mapToInt((x) -> labelA.get(x) * labelB.get(x))
				.sum();
	}
	
	public static double magnitude (RIV label) {
		return Math.sqrt(
				label.values()
				.parallelStream()
				.mapToInt((x) -> x ^ 2)
				.sum());
	}
	
	public static double similarity (RIV labelA, RIV labelB) {
		final double mag = magnitude(labelA) * magnitude(labelB);
		return (mag == 0)
				? 0
				: dotProduct(labelA, labelB) / mag;
	}
	
	public static RIV addLabels (RIV labelA, RIV labelB) {
		RIV result = new RIV();
		BiConsumer<Integer, Integer> f = ((k, v) -> result.merge(k, v, ((x, y) -> x + y))); 
		labelA.forEach(f);
		labelB.forEach(f);
		return result;
	}
	
	public static List<Integer> makeKs (int k, long seed) {
		List<Integer> l = new ArrayList<>();
		for (int i = 0; i < k; i++) 
			l.add((i < k / 2) 
					? 1 
					: -1);
		return Util.shuffleList(l, seed);
	}
	
	public static List<Integer> makeIndices (int size, int k, long seed) {
		return new ArrayList<>(Util.randIntList(size, k, seed));
	}
	
	public static long makeSeed (String word) {
		System.out.println("makeString called with arg: " + word);
		return word.chars()
				.asLongStream()
				.map((x) -> x * (10 ^ (word.indexOf((int)x))))
				.sum();
	}
	
	public static int makeEven (int x) { 
		return (x % 2 == 0) ? x : x + 1; 
		}
	
	public static RIV generateLabel (int size, int k, String word) {
		System.out.println("generateLabels called with args\nSize: " + size +
							" K: " + k +
							" Word: " + word);
		RIV res = new RIV();
		long seed = makeSeed(word);
		int j = makeEven(k);
		List<Integer> ks = makeKs(j, seed);
		List<Integer> is = makeIndices(size, j, seed);
		is.forEach((x) -> res.put(x, ks.get(is.indexOf(x))));
		return res;
	}	
}
