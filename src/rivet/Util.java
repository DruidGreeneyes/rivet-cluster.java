package rivet;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import rivet.util.Counter;
import scala.Tuple2;
import testing.Log;


public final class Util {	
	private static final Log log = new Log("test/utilOutput.txt");
	
	private Util(){}
	
	public static String concatStrings (String a, String b) {
		return String.join("", a, b);
	}
	
	public static <A, R> List<R> mapToList (final A[] arr, final Function<A, R> fun) {
		return mapList(fun, Arrays.asList(arr));
	}
	
	public static <K, M extends Map<K, ?>> Set<K> getMatchingKeys (final M hashA, final M hashB) {
		return hashA.keySet()
				.parallelStream()
				.filter((x) -> hashB.containsKey(x))
				.collect(Collectors.toSet());
	}
	
	public static <T> List<T> shuffleList(final List<T> lis, final Long seed) {
		log.log("Shuffling list - Seed: %d", seed);
		log.log("List: %s", lis.toString());
		Instant t = Instant.now();
		final Integer size = lis.size();
		List<T> res = new Random(seed)
						.ints(0, size)
						.distinct()
						.limit(size)
						.mapToObj(lis::get)
						.collect(Collectors.toList());
		log.log("shuffleList returns, %s elapsed:%n%s",
				Util.timeSince(t),
				res.toString());
		return res;
	}
	
	public static IntStream randInts (final Integer bound, final Integer length, final Long seed) {
	log.log("Generating Random Integers - Bound: %d, Length: %d, Seed: %d",
				bound, length, seed);
		Instant t = Instant.now();
		IntStream res = new Random(seed).ints(0, bound).distinct().limit(length);
		log.log("randInts returns, %s elapsed: %s",
				Util.timeSince(t), res.toString());
		return res;
	}
	
	public static List<Integer> randIntList(final Integer bound, final Integer length, final Long seed) {
		log.log("Generating Rand Int List - Bound: %d, Length: %d, Seed: %d",
				bound, length, seed);
		Instant t = Instant.now();
		List<Integer> res = randInts(bound, length, seed)
								.boxed()
								.collect(Collectors.toList());
		log.log("randIntList returns, %s elapsed:%n%s",
				Util.timeSince(t),
				res.toString());
		return res;
	}
	
	public static List<Integer> permuteIntList(final List<Integer> lis, final Long increment, final Integer bound) {
		log.log("Permuting List from %d to %d:%n%s",
				increment,
				increment + 1,
				lis.toString());
		Instant t = Instant.now();
		final Counter c = new Counter(-1);
		List<Integer> nums = new ArrayList<>();
		List<Tuple2<Integer, Integer>> tups = new ArrayList<>();
		final List<Integer> res = 
				new Random(increment).ints(0, bound)
				.distinct()
				.limit(bound)
				.mapToObj((i) -> {
					nums.add(i);
					Tuple2<Integer, Integer> entry = Tuple2.apply(c.inc().intValue(), i);
					tups.add(entry);
					return entry;
				})
				.filter((e) -> lis.contains(e._1))
				.map(Tuple2::_2)
				.collect(Collectors.toList());
		log.log("Nums: %s", nums.toString());
		log.log("Tups: %s", tups.toString());
		log.log("permuteIntList returns, %s elapsed:%n%s",
				Util.parseDuration(Duration.between(t, Instant.now())),
				res.toString());
		return res;
	}
	
	public static List<Integer> dePermuteIntList(final List<Integer> lis, final Long increment, final Integer bound) {
		log.log("Depermuting List from %d to %d:%n%s",
				increment + 1,
				increment,
				lis.toString());
		Instant t = Instant.now();
		final Counter c = new Counter(-1);
		List<Integer> nums = new ArrayList<>();
		List<Tuple2<Integer, Integer>> tups = new ArrayList<>();
		final List<Integer> res = 
			new Random(increment).ints(0, bound)
				.distinct()
				.limit(bound)
				.mapToObj((i) -> {
					nums.add(i);
					Tuple2<Integer, Integer> entry = Tuple2.apply(c.inc().intValue(), i);
					tups.add(entry);
					return entry;
				})
				.filter((e) -> lis.contains(e._2))
				.map(Tuple2::_1)
				.collect(Collectors.toList());
		log.log("Nums: %s", nums.toString());
		log.log("Tups: %s", tups.toString());
		log.log("dePermuteIntList returns, %s elapsed:%n%s",
				Util.parseDuration(Duration.between(t, Instant.now())),
				res.toString());
		return res;
	}
	
	public static <T> List<T> setToList (final Set<T> s) {
		return s.parallelStream().collect(Collectors.toList());
	}
	
	public static <A, R> Set<R> mapSet (final Function<A, R> fun, final Set<A> set) {
		return set.parallelStream().map(fun).collect(Collectors.toSet());
	}
	
	public static <T, R> List<R> mapList(final Function<T, R> fun, final List<T> lis) {
		return lis.parallelStream().map(fun).collect(Collectors.toList());
	}
	
	
	public static LongStream range (final Long start, final Long bound, final Long step) {
		final Long size = (bound - start) / step;
		return LongStream.iterate(start, (acc) -> acc + step).limit(size);
	}
	public static LongStream range (final Long start, final Long bound) { return LongStream.range(start, bound);}
	public static LongStream range(final Long bound) { return range(0L, bound); }
	
	public static IntStream range (final Integer start, final Integer bound, final Integer step) { 
		final Integer size = (bound - start) / step;
		return IntStream.iterate(start, (acc) -> acc + step).limit(size);
	}
	public static IntStream range (final Integer start, final Integer bound) { return IntStream.range(start, bound); }
	public static IntStream range (final Integer bound) {	return range(0, bound); }
	
	public static IntStream butCenter (final IntStream s, final Integer radius) {
		return IntStream.concat(s.limit(radius), s.skip(s.count() - radius));
	}
	
	public static IntStream rangeNegToPos (final Integer c) { return range(0 - c, c + 1); }
	
	public static List<String> getContextWindow (final Integer index, final Integer cr, final List<String> text) {
		final Integer s = text.size();
		return butCenter(rangeNegToPos(cr), cr)
					.parallel()
					.map((x) -> x + index)
					.filter((x) -> x >= 0 && x < s)
					.mapToObj((x) -> text.get(x))
					.collect(Collectors.toList());
	}
	
	private static <T> List<T> takeBy (final Integer num, final Function<T, Double> keyfn, final ListIterator<T> lis, final TreeMap<Double, T> res) {
		while (lis.hasNext()) {
			final T item = lis.next();
			final Double ret = keyfn.apply(item);
			if (res.floorKey(ret) != null)
				res.put(ret,  item);
			while(res.size() >= num) 
				res.remove(res.firstKey());
		}
		return new ArrayList<>(res.descendingMap().values());
	}
	public static <T> List<T> takeBy (final Integer num, final Function<T, Double> keyfn, final List<T> lis) {
		final TreeMap<Double, T> res = new TreeMap<>();
		final T item = lis.get(0);
	    res.put(keyfn.apply(item), item);
		return takeBy(num, keyfn, lis.listIterator(), res);
	}
	
	public static Map<Integer, String> index (final List<String> tokens, final Integer count) {
		final Map<Integer, String> res = new HashMap<>();
		Util.range(count).forEach((i) -> 
			res.put(i, tokens.get(i)));
		return res;
	}
	
	public static <T> JavaPairRDD<Long, T> index (final JavaRDD<T> tokens) {
		return tokens.zipWithIndex().mapToPair(Tuple2::swap);
	}
	
	public static String parseTimeString (final String timestring) {
		String ts = timestring.substring(2);
		if (ts.contains("S"))
			ts = ts.substring(0, ts.indexOf("S"));
		if (ts.contains("H"))
			ts = ts.replace("H", ":");
		else
			ts = "0:" + ts;
		if (ts.contains("M"))
			ts = ts.replace("M", ":");
		else
			ts = ts.substring(0, ts.indexOf(":") + 1) + "0:" + ts.substring(ts.indexOf(":") + 1);
		return ts;
	}
	
	public static String parseDuration (final Duration duration) {
		return parseTimeString(duration.toString());
	}
	
	public static String timeSince (final Instant start) {
		return parseDuration(Duration.between(start, Instant.now()));
	}
}
