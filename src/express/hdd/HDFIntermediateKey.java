package express.hdd;

import org.apache.hadoop.io.Text;

public class HDFIntermediateKey {
	public static Text merge(Text naturalKey, Text secondaryKey) {
		return new Text(naturalKey.toString() + "+" + secondaryKey.toString());
	}
	
	public static Text getNaturalKey(Text IntermediateKey){
		return new Text(IntermediateKey.toString().split("+")[0]);
	}
	
	public static Text getSecondaryKey(Text IntermediateKey){
		return new Text(IntermediateKey.toString().split("+")[1]);
	}
}
