package express.hdd;

import java.util.Arrays;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;

public class HDFOutputKeyComparator implements RawComparator<Text> {

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		//return compare(new Text(Arrays.copyOfRange(b1, s1, l1)), new Text(Arrays.copyOfRange(b2, s2, l2)));
		return compare(new Text(b1), new Text(b2));
	}

	@Override
	public int compare(Text tx, Text ty) {
		try {
			System.out.printf("HDFOutputKeyComparator: x=%s and y=%s\n", tx.toString(), ty.toString());
			Pair<int[], int[]> x = Tools.text2Pair(HDFIntermediateKey.getSecondaryKey(tx));
			Pair<int[], int[]> y = Tools.text2Pair(HDFIntermediateKey.getSecondaryKey(ty));
				
			if (HyperRectangleData.hasIncreasingChunkNumber(x, y))
				return -1;
			else
				return 1;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 0;
	}
	
}
