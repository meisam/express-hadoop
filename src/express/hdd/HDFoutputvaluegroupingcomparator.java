package express.hdd;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

public class HDFoutputvaluegroupingcomparator implements RawComparator<Text> {

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return compare(new Text(b1), new Text(b2));
	}

	@Override
	public int compare(Text tx, Text ty) {
		byte[] b1 = HDFIntermediateKey.getNaturalKey(tx).getBytes();
		byte[] b2 = HDFIntermediateKey.getNaturalKey(ty).getBytes();
		return Text.Comparator.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
	}

}
