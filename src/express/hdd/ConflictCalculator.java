package express.hdd;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import express.hdd.HDFMicroBenchmark.HDFMicroMapper;
import express.hdd.HDFMicroBenchmark.HDFMicroReducer;

/**
 * Generate a partition conflict summary 
 * All partitioning file must be stored in the same folder
 * To run the program: 
 * <b>bin/hadoop jar express-hadoop.jar express.hdd.ConflictCalculator 
 * 		[InputDir] [OutputDirectory]</b>
 */

public class ConflictCalculator extends Configured implements Tool {
	public static class CCMapper extends MapReduceBase 
    implements Mapper<Text, Text, Text, Text> {
		
		String nodeIP;
		public void configure(JobConf conf) {
		     try {		 
		    	 System.out.printf("start mapper-configure\n");
		    	 InetAddress addr = InetAddress.getLocalHost();
		    	 nodeIP = addr.getHostAddress();
		    	 System.out.printf("Got nodeIP = %s\n", nodeIP);
		     } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
		     }
		}
		
	    public void map(Text key, Text value,
              OutputCollector<Text, Text> fout,
              Reporter reporter) throws IOException {
	    	
	    	System.out.printf("start mapper\n");
	    	fout.collect(new Text(nodeIP), key);	
	    }
	  }

	public static class CCReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		Path OutputDirectory;
		protected FileSystem fs;
		private JobConf job;
		
		public void configure(JobConf job) {
		     try {
		    	 System.out.printf("start reducer-configure\n");
		    	 OutputDirectory=  new Path(job.get("OutputDirectory").toString());
		    	 this.job=job;
		    	 fs = FileSystem.get(job);
		     } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		@Override
		public void reduce(Text nodeID, Iterator<Text> keys,
				OutputCollector<Text, Text> fout, Reporter arg3)
				throws IOException {
			
			System.out.printf("start reducer\n");
			
			Path nodeSummary = new Path(OutputDirectory, nodeID.toString());
			final SequenceFile.Writer writer = SequenceFile.createWriter(
		              fs, job, nodeSummary, Text.class, Text.class, CompressionType.NONE);
			
			ArrayList<Pair<int[], int[]>> recs = new ArrayList<Pair<int[], int[]>>(); 
			while (keys.hasNext()){
				Text key = keys.next();
				//writer.append(key, new Text("1")); //intersection(recs), #rec 
				try {
					recs.add(Tools.text2Pair(key));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			for (int i=0; i<recs.size(); i++)
				for (int j=i+1; j<recs.size(); j++){
					Pair<int[], int[]> conflict = HyperRectangleData.getHyperRectangleIntersection(recs.get(i), recs.get(j));
					if (conflict != null)
						try {
							//writer.append(Tools.pair2Text(conflict), new Text("2"));
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
				}
			
			ArrayList<ArrayList<Pair<int[], int[]>>> conflictList = new ArrayList<ArrayList<Pair<int[], int[]>>> ();
			ArrayList<ArrayList<Set<Integer>>> conflictID = new ArrayList<ArrayList<Set<Integer>>> ();
			calculateConflicts(recs, conflictList, conflictID);
			try {
				dumpConflictList(conflictList, writer);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			writer.close();
		}
	}
	
	private static void calculateConflicts(ArrayList<Pair<int[], int[]>> recs, 
			ArrayList<ArrayList<Pair<int[], int[]>>> conflictList, ArrayList<ArrayList<Set<Integer>>> conflictID) {
		ArrayList<Set<Integer>> recIDList1 = new ArrayList<Set<Integer>>();
		for (int i=0; i<recs.size(); i++){
			recIDList1.add(new HashSet<Integer>(Arrays.asList(i)) );
		}
		
		conflictList.add((ArrayList<Pair<int[], int[]>>) recs.clone()); //0: all rectangles
		conflictID.add(recIDList1);
		
		ArrayList<Pair<int[], int[]>> Conflict2List = new ArrayList<Pair<int[], int[]>>();
		ArrayList<Set<Integer>> recIDList2 = new ArrayList<Set<Integer>>();
		for (int i=0; i<recs.size(); i++)
			for (int j=i+1; j<recs.size(); j++){
				Pair<int[], int[]> conflict = HyperRectangleData.getHyperRectangleIntersection(recs.get(i), recs.get(j));
				if (conflict != null)
					try {
						Conflict2List.add(conflict);
						recIDList2.add(new HashSet<Integer>(Arrays.asList(i,j)) );
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
			}
			
	}
	
	private static void dumpConflictList(ArrayList<ArrayList<Pair<int[], int[]>>> conflictList, 
			SequenceFile.Writer writer) throws IOException, Exception {
		if (conflictList == null)
			return;
		for (int i=0; i<conflictList.size(); i++) {
			ArrayList<Pair<int[], int[]>> list = conflictList.get(i);
			for (int j=0; j<list.size(); j++){
				writer.append(Tools.pair2Text(list.get(j)), new Text(Integer.toString(i+1)));
			}
		}
	}
		
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		job.set("InputDirectory", args[0]);
		job.set("OutputDirectory", args[1]);
		job.setJobName("ConflictCalculator");
		job.setJarByClass(ConflictCalculator.class);
		job.setMapperClass(CCMapper.class);
	    job.setReducerClass(CCReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    final Path inDir = new Path(args[0]);
	    final Path outDir = new Path(args[1]);
	    FileOutputFormat.setOutputPath(job, outDir);
	    job.setInputFormat(SequenceFileAsTextInputFormat.class);
	    
	    addAllFiles(job, inDir); //adding folders will yield FileNotFoundException (looking for ${PATH}/data)
		
		JobClient.runJob(job);
		return 0;
	}
	
	private static void addAllFiles(JobConf job, Path in) {
		FileSystem fs;
		try {
			fs = FileSystem.get(job);
		    FileStatus[] files = fs.listStatus(in);
		    for (int i=0;i<files.length;i++){
		    	FileStatus file = files[i];
		    	if (file.isDir()) {
		    		addAllFiles(job, file.getPath());
		    	} else {
		    		FileInputFormat.addInputPath(job, file.getPath());
		    	}
		    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new ConflictCalculator(), args);
	    System.exit(res);
	}
}
