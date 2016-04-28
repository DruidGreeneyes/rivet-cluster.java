package rivet.extras.text;

import static java.util.Arrays.stream;

import org.apache.commons.lang3.ArrayUtils;

import rivet.core.arraylabels.*;

public class Shingles {
	
	public static String[] shingleText(String text, int width, int offset) {
		String[] res = new String[0];
		for (int i = 0; i < text.length(); i += offset)
			res = ArrayUtils.add(res, text.substring(i, i + width));
		return res;
	}
	
	public static RIV[] rivShingles (String[] shingles, int size, int k) {
		return stream(shingles)
			.map(Labels.labelGenerator(size, k))
			.toArray(RIV[]::new);
	}
	
	public static RIV sumRIVs (RIV[] rivs) { return Labels.addLabels(rivs); }
	
	public static RIV rivettizeText(String text, int width, int offset, int size, int k) {
		return sumRIVs(rivShingles(shingleText(text, width, offset), size, k));
	}
}
