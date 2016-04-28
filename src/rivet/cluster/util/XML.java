package rivet.cluster.util;

public class XML {
	public static String getTagContents(String text, String tag) {
		String open = text.split("<" + tag + ">", 2)[1];
		return open.split("</" + tag + ">", 2)[0];
	}
}
