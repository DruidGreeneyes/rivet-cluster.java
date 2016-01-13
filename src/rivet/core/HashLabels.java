package rivet.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import rivet.Util;
import rivet.core.RIV;
import testing.Log;

public final class HashLabels {
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
		labelB.entrySet().parallelStream().forEach(result::mergePlus);
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
		return new TreeSet<>(Util.randIntList(size, k, seed));
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
		Instant t = Instant.now();
		log.log("Generate Labels called. Size: %d, K: %d, Word: %s", size, k, word);
		log.log("Making seed from word.");
		final Long seed = makeSeed(word);
		log.log("Seed: %d. Ensuring K is even.", seed);
		final Integer j = makeEven(k);
		log.log("K: %d, Generating random Ks.", j);
		final List<Double> ks = makeKs(j, seed);
		log.log("Random Ks: %s, Generating random Indices.", ks.toString());
		final SortedSet<Integer> is = makeIndices(size, j, seed);
		log.log("Random Indices: %s, Splicing new RIV.", is.toString());
		RIV res = new RIV(is, ks, size, k);
		log.log("Generate Labels returns, %s elapsed: %n%s", 
				Util.timeSince(t),
				res.toString());
		return res;
		
	}
}
