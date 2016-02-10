package rivet.util;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import rivet.cluster.spark.Setting;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import scala.Tuple2;
import testing.Log;


public final class Util {	
	@SuppressWarnings("unused")
	private static final Log log = new Log("test/utilOutput.txt");
	
	private Util(){}
	
	public static <T> Optional<T> gOptToJOpt (com.google.common.base.Optional<T> gopt) {
		return Optional.ofNullable(gopt.orNull());
	}
	
	public static Setting setting(String key, String val) {
		return new Setting(key, val);
	}
	
	public static <T> Optional<T> mergeOptions(Optional<T> optA, Optional<T> optB, BiFunction<T, T, T> mergeFunction) {
		if (optA.isPresent() && optB.isPresent()) 
			return Optional.of(mergeFunction.apply(optA.get(), optB.get()));
		if (optA.isPresent()) return optA;
		if (optB.isPresent()) return optB;
		return Optional.empty();
	}
	
	public static <T> Optional<T> getOpt(List<T> lis, Integer index) {
		try {
			return Optional.ofNullable(lis.get(index));
		} catch (IndexOutOfBoundsException e) {
			return Optional.empty();
		}
	}
	
	public static <K, M extends Map<K, ?>> Set<K> getMatchingKeys (final M hashA, final M hashB) {
		return hashA.keySet()
				.parallelStream()
				.filter((x) -> hashB.containsKey(x))
				.collect(toSet());
	}
	
	public static <T> List<T> shuffleList(final List<T> lis, final Long seed) {
		final Integer size = lis.size();
		return randInts(size, size, seed)
				.mapToObj(lis::get)
				.collect(toList());
	}
	public static int[] shuffleIntArray(final int[] arr, final Long seed) {
		final int size = arr.length;
		return randInts(size, size, seed)
				.map((i) -> arr[i])
				.toArray();
	}
	public static double[] shuffleDoubleArray(final double[] arr, final Long seed) {
		final int size = arr.length;
		return randInts(size, size, seed)
				.mapToDouble((i) -> arr[i])
				.toArray();
	}
	
	public static IntStream randInts (final Integer bound, final Integer length, final Long seed) {
		return new Random(seed).ints(0, bound).distinct().limit(length);
	}
	
	public static LongStream range (final Long start, final Long bound) { return LongStream.range(start, bound);}
	public static LongStream range (final Long start, final Long bound, final Long step) {
		return range(start, bound).filter((x) -> (x - start) % step == 0L);
	}
	public static LongStream range(final Long bound) { return range(0L, bound); }
	
	public static IntStream range (final Integer start, final Integer bound, final Integer step) { 
		return range(start, bound).filter((x) -> (x - start) % step == 0L);
	}
	public static IntStream range (final Integer start, final Integer bound) { return IntStream.range(start, bound); }
	public static IntStream range (final Integer bound) {	return range(0, bound); }
	
	public static IntStream butCenter (final IntStream s, final Integer radius) {		
		Counter c = new Counter();
		return s.mapToObj(i -> new Tuple2<>(i, c.inc()))
				.filter(e -> (e._2 < radius || e._2 > c.get() - radius))
				.mapToInt(Tuple2::_1);
	}
	
	public static IntStream rangeNegToPos (final Integer c) { return range(0 - c, c + 1); }
	
	public static List<String> getContextWindow (final Integer index, final Integer cr, final List<String> text) {
		final Integer s = text.size();
		return butCenter(rangeNegToPos(cr), cr)
					.map((x) -> x + index)
					.filter((x) -> x >= 0 && x < s)
					.mapToObj((x) -> text.get(x))
					.collect(toList());
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
