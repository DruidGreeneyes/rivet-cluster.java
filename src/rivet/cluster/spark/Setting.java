package rivet.cluster.spark;

import scala.Tuple2;

public class Setting extends Tuple2<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8698427019682258793L;
	public Setting(String key, String value) {super(key, value);}
	public String key() {return this._1;}
	public String value() {return this._2;}

}
