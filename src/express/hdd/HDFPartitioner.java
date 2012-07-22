package express.hdd;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.Text;

public class HDFPartitioner implements Partitioner<Text,Text> {
	private static int [] dataSize;
	private static int [] coffset;
	private static int [] clength;
	private static int dimension;
	private static HyperRectangleData recData;
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		int [] offset = {};
		try {
			Pair<int[], int[]> keyPair = Tools.text2Pair(HDFIntermediateKey.getNaturalKey(key));
			offset = keyPair.getLeft();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 
		}
		int rnum = recData.getChunkNumber(offset);
		//System.out.println("Partitioner:: offset:" + Arrays.toString(offset) + ", clength:" + 
		//		Arrays.toString(clength) + ", dataSize:" + Arrays.toString(dataSize) + ", #reducer:" + rnum);
		return rnum;
	}
 
	@Override
	public void configure(JobConf job) {
		try {
			dataSize = Tools.StringArray2IntArray(job.get("DataSize").toString().split(","));
			coffset = Tools.StringArray2IntArray(job.get("ChunkOffset").toString().split(","));
			clength = Tools.StringArray2IntArray(job.get("ChunkLength").toString().split(","));
			dimension = dataSize.length;
			recData = new HyperRectangleData(dataSize, coffset, clength, dimension); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 
}
