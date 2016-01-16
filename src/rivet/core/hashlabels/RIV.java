package rivet.core.hashlabels;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Properties;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;
import rivet.util.Util;
import scala.Tuple2;
import testing.Log;

public class RIV extends HashMap<Integer, Double> {
	private static final long serialVersionUID = 7549131075767220565L;
	/**
	 * 
	 */
	//@SuppressWarnings("unused")
	private static final Log log = new Log("test/rivOutput.txt");
	private final Properties props;
	
	public RIV () { super(); this.props = new Properties();}
	public RIV (final RIV riv) {
		super(riv);
		this.props = new Properties();
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
		final List<Integer> ks = new ArrayList<>(keys);
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
		//TODO This seems to throw NPEs unless I log or print e. Why!?
		//log.log("mergePlus:" + e);
		//log.logEmpty();
		this.mergePlus(e.getKey(), e.getValue());
	}
	
	public void mergeMinus (final Integer key, final Double value) {
		final Double v = -value;
		this.merge(key, v, Double::sum);
	}
	public void mergeMinus (final Entry<Integer, Double> e) {
		this.mergeMinus(e.getKey(), e.getValue());
	}
	
	public void put (final Entry<Integer, Double> e) {
		this.put(e.getKey(), e.getValue());
	}
	
	public int[] keyArray () { return this.keySet().stream().mapToInt(x -> x.intValue()).toArray(); }
	public int[] valArray () { return this.values().stream().mapToInt(x -> x.intValue()).toArray(); }
	
	private RIV permuteLoop (int[] permutation, int times) {
		log.log("Permuting -- permutation: %s, times: %s",
				stream(permutation).boxed().collect(Collectors.toList()),
				times);
		int[] keys = this.keyArray();
		for (int i = 0; i < times; i++)
			for (int c = 0; c < keys.length; c++)
				keys[c] = permutation[keys[c]];
		return new RIV(
				stream(keys).boxed().collect(Collectors.toSet()),
				this.values(),
				this.getVectorSize(),
				this.getVectorK());
	}
	
	public RIV permute (Tuple2<int[], int[]> permutations, int times) {
		return (times == 0)
				? this
						: (times > 0)
						? permuteLoop(permutations._1, times)
								: permuteLoop(permutations._2, -times);
	}

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
								.map((v) -> v / mag)
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
