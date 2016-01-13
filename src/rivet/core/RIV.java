package rivet.core;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

import rivet.Util;
import testing.Log;

public class RIV extends TreeMap<Integer, Double> {
	private static final long serialVersionUID = 7549131075767220565L;
	/**
	 * 
	 */
	private static final Log log = new Log("test/rivOutput.txt");
	private final Properties props = new Properties();
	
	public RIV () { super(); }
	public RIV (final RIV riv) {
		super(riv);
		this.props.put("Size", riv.getProperty("Size"));
		this.props.put("K", riv.getProperty("K"));
	}
	public RIV (final Integer vectorSize, final Integer vectorK) {
		this();
		this.props.put("Size", vectorSize);
		this.props.put("K", vectorK);
	}
	
	public RIV (final Set<Integer> keys, final Collection<Double> values, final Integer size, final Integer k) {
		this(size, k);
		final List<Integer> ks = keys.stream().sorted().collect(Collectors.toList());
		final List<Double> vs = new ArrayList<>(values);
		Util.range(ks.size())
				.forEach((i) -> this.put(ks.get(i), vs.get(i)));
	}
	
	public RIV (final String mapString) {
		this();
		final String[] mapstr = mapString.trim()
							.split("\\|");
		Arrays.stream(
				mapstr[0].substring(1, mapstr[0].length() - 1)
					.split("\\s+"))
			.map((x) -> x.split("="))
			.forEach((entry) -> this.put(
						Integer.parseInt(entry[0]),
						Double.parseDouble(entry[1])));
		Arrays.stream(mapstr[1].split("\\s+"))
			.map((x) -> x.split("="))
			.forEach((entry) -> this.props.put(entry[0], entry[1]));
	}
	
	public String toString() {
		return super.toString() + "|" + this.props.toString();
	}
	
	public <T extends Object> void setProperty (final String property, final T val) {
		this.props.put(property, val);
	}
	
	public Object getProperty(final String property) {
		return this.props.get(property);
	}
	
	public Integer getVectorSize() {
		return Integer.parseInt(this.getProperty("Size").toString());
	}
	
	public Integer getVectorK() {
		return Integer.parseInt(this.getProperty("K").toString());
	}
	
	public void mergePlus (final Integer key, final Double value) {
		this.merge(key, value, Double::sum);
	}
	public void mergePlus (final Entry<Integer, Double> e) {
		this.mergePlus(e.getKey(), e.getValue());
	}
	
	public void mergeMinus (final Integer key, final Double value) {
		final Double v = 0 - value;
		this.merge(key, v, Double::sum);
	}
	public void mergeMinus (final Entry<Integer, Double> e) {
		this.mergeMinus(e.getKey(), e.getValue());
	}
	
	public void put (final Entry<Integer, Double> e) {
		this.put(e.getKey(), e.getValue());
	}
	
	private SortedSet<Integer> permuteDown (final Long start, final Long end, final Set<Integer> keys, final Integer size) {
		log.log("Permuting keys down from %d to %d:%n%s",
				start,
				end,
				keys.toString());
		Instant t = Instant.now();
		Long i = start;
		List<Integer> ks = new ArrayList<>(keys);
		while (i > end) {
			i--;
			log.log("Current i = %d; calling dePermuteList.", i);
			ks = Util.dePermuteIntList(ks, i, size);
		}
		log.log("permuteDown returns, %s elapsed:%n%s",
				Util.timeSince(t),
				ks.toString());
		return new TreeSet<>(ks);
	}
	
	private SortedSet<Integer> permuteUp (final Long start, final Long end, final Set<Integer> keys, final Integer size) {
		log.log("Permuting keys up from %d to %d:%n%s",
				start,
				end,
				keys.toString());
		Instant t = Instant.now();
		Long i = start;
		List<Integer> ks = new ArrayList<>(keys);
		while (i < end) {
			log.log("Current i = %d; calling permuteList.", i);
			ks = Util.permuteIntList(ks, i, size);
			i++;
		}
		log.log("permuteUp returns, %s elapsed:%n%s",
				Util.timeSince(t),
				ks.toString());
		return new TreeSet<>(ks);
	}
	
	public RIV permute (final Number start, final Number end) {
		final Long s = start.longValue();
		final Long e = end.longValue();
		if (s - e == 0) return this;
		final Set<Integer> res = (s < e)
				? permuteUp(s, e, this.keySet(), this.getVectorSize())
				: permuteDown(s, e, this.keySet(), this.getVectorSize());
		return new RIV(
				res,
				this.values(),
				this.getVectorSize(),
				this.getVectorK());
	}
	
	public RIV permuteFromZero (final Number times) { return permute(0, times); }
	public RIV permuteToZero (final Number times) { return permute(times, 0); }
	
	public Double magnitude () {
		return Math.sqrt(
				this.values()
				.parallelStream()
				.mapToDouble((x) -> x * x)
				.sum());
	}
	
	public RIV normalize () {
		final Double mag = this.magnitude();
		final List<Double> vals = this.values()
								.parallelStream()
								.mapToDouble((v) -> v / mag)
								.boxed()
								.collect(Collectors.toList());
		return new RIV(
				this.keySet(),
				vals,
				this.getVectorSize(),
				this.getVectorK());
	}
	
	public RIV subtract (RIV riv) {
		final RIV result = new RIV(this);
		riv.entrySet().parallelStream().forEach(result::mergeMinus);
		return result;
	}
	
	public RIV divideBy (Long num) {
		List<Double> newVals = this.values()
								.parallelStream()
								.map((val) -> val / num)
								.collect(Collectors.toList());
		return new RIV(
				this.keySet(),
				newVals,
				this.getVectorSize(),
				this.getVectorK());
	}
}
