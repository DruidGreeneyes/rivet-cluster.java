package rivet.cluster.spark;

import java.util.TreeMap;

public class Row extends TreeMap<String, String>{
	public Row(){super();}
	public Row(Row row) {super(row);}
	public Row(TreeMap<String, String> map) {super(map);}

	/**
	 * 
	 */
	private static final long serialVersionUID = -8268389694010515839L;
	
}
