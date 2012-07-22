package express.hdd;

import java.util.ArrayList;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.io.Text;

public class HDFSetQueryPartitioner implements Partitioner<Text,Text> {
	ArrayList<int[]> offsets;
	ArrayList<int[]> lengths;
	
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		try {
			Pair<int[], int[]> inputChunk = Tools.text2Pair(HDFIntermediateKey.getNaturalKey(key));
			for (int i=0; i<offsets.size(); i++) {
				if (Tools.compareChunk(inputChunk, new Pair<int[], int[]>(offsets.get(i), lengths.get(i)) ) == 0)
					return i;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 
		}
		return 0;
	}
 
	@Override
	public void configure(JobConf job) {
		try {
			offsets = Tools.str2IntArrayList(job.get("ChunkOffsets"));
			lengths = Tools.str2IntArrayList(job.get("ChunkLengths"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 
}
