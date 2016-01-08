package rivet;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;


public final class Util {	
	private Util(){}
	
	public static <A, R> List<R> mapToList (A[] arr, Function<A, R> fun) {
		return mapList(fun, Arrays.asList(arr));
	}
	
	public static <K> Set<K> getMatchingKeys (HashMap<K, ?> hashA, HashMap<K, ?> hashB) {
		return hashA.keySet()
				.parallelStream()
				.filter((x) -> hashB.containsKey(x))
				.collect(Collectors.toSet());
	}
	
	public static <T> List<T> shuffleList(List<T> lis, Long seed) {
		List<T> res = new ArrayList<>(lis);
		Random r = new Random(seed);
		Integer s = lis.size();
		for (int c = 0; c < 10; c ++) {
			List<T> l = new ArrayList<>(res);
			res.forEach((item) -> l.set(r.nextInt(s), item));
			res = l;
		}
		return res; 
	}
	
	public static Set<Integer> randIntList(Integer bound, Integer length, Long seed) {
		Random r = new Random(seed);
		Set<Integer> res = new HashSet<>();
		while (res.size() < length) {
			res.add(r.nextInt(bound));
		}
		return res;
	}
	
	public static <T> List<T> setToList (Set<T> s) {
		return s.stream().collect(Collectors.toList());
	}
	
	
	public static <T, R> List<R> mapList(Function<T, R> fun, List<T> lis) {
		return lis.parallelStream().map(fun).collect(Collectors.toList());
	}
	
	public static List<Long> range (Long start, Long bound, Long step) {
		List<Long> ret = new ArrayList<>();
		if (step != 0L) {
			LongPredicate p = (step > 0L) 
					? (x) -> x < bound 
					: (x) -> x > bound;
			for (Long i = start; p.test(i); i += step) 
				ret.add(i);
		}
		return ret;
	}
	public static List<Long> range (Long start, Long bound) { 
		return (start == bound)
				? new ArrayList<>()
						: range(start, bound, (start < bound)
												? 1L
												: -1L);
	}
	public static List<Long> range(Long bound) { return range(0L, bound); }
	
	public static List<Integer> range (Integer start, Integer bound, Integer step) { 
		return mapList((x) -> x.intValue(),
				range(start.longValue(),bound.longValue(),step.longValue()));
	}
	public static List<Integer> range (Integer start, Integer bound) { 
		return mapList((x) -> x.intValue(),
				range(start.longValue(), bound.longValue()));
	}
	public static List<Integer> range (Integer bound) {	
		return mapList((x) -> x.intValue(),
				range(bound.longValue()));
	}
	
	public static <T> List<T> butCenter (List<T> arr, Integer radius) {
		Integer size = arr.size();
		return mapList(
				(index) -> arr.get((index < radius)
								? index
										: (size + index) - (radius * 2)),
				range(radius * 2));
	}
	
	public static List<Integer> rangeNegToPos (Integer c) {
		return range(0 - c, c + 1);
	}
	
	public static List<String> getContextWindow (Integer index, Integer cr, List<String> text) {
		Integer s = text.size();
		return butCenter(rangeNegToPos(cr), cr)
					.stream()
					.map((x) -> x + index)
					.filter((x) -> x >= 0 && x < s)
					.map((x) -> text.get(x))
					.collect(Collectors.toList());
	}
	
	private static <T> List<T> takeBy (Integer num, Function<T, Double> keyfn, ListIterator<T> lis, TreeMap<Double, T> res) {
		while (lis.hasNext()) {
			T item = lis.next();
			double ret = keyfn.apply(item);
			if (res.floorKey(ret) != null)
				res.put(ret,  item);
			while(res.size() >= num) 
				res.remove(res.firstKey());
		}
		return new ArrayList<>(res.descendingMap().values());
	}
	public static <T> List<T> takeBy (Integer num, Function<T, Double> keyfn, List<T> lis) {
		TreeMap<Double, T> res = new TreeMap<>();
		T item = lis.get(0);
	    res.put(keyfn.apply(item), item);
		return takeBy(num, keyfn, lis.listIterator(), res);
	}
	
	public static Map<Long, String> index (List<String> tokens, Long count) {
		Map<Long, String> res = new HashMap<>();
		Util.range(count).forEach((i) -> 
			res.put(i, tokens.get(i.intValue())));
		return res;
	}
	
	public static <T> JavaPairRDD<Long, T> index (JavaRDD<T> tokens) {
		return tokens.zipWithIndex().mapToPair(Tuple2::swap);
	}
	
	public static String parseTimeString (String timestring) {
		String ts = timestring.substring(2);
		if (ts.contains("S"))
			ts = ts.substring(0, ts.indexOf("S"));
		if (ts.contains("."))
			ts = ts.substring(0, ts.indexOf("."));
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
	
	public static <I extends Number, N extends Number> void printTimeEntry (long startTimeMillis, I index, N count) {
		long n = System.currentTimeMillis();
		long c = count.longValue();
		long i = index.longValue() + 1;
		Duration et = Duration.ofMillis(n - startTimeMillis);
		Duration rt = Duration.ofMillis((c - i) * (n / i));
		System.out.println(String.format("Word %d out of %d: %d%% of file",
											i, c, (i/ c)));
		System.out.println(String.format("%s elapsed, estimate %s remaining",
											Util.parseTimeString(et.toString()),
											Util.parseTimeString(rt.toString())));
	}
}
