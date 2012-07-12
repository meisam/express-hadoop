package express.hdd;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapred.TaskAttemptID;
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
		    	 InetAddress addr = InetAddress.getLocalHost();
		    	 nodeIP = addr.getHostAddress();
		     } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
		     }
		}
		
	    public void map(Text key, Text value,
              OutputCollector<Text, Text> fout,
              Reporter reporter) throws IOException {
	    	
	    	fout.collect(new Text(nodeIP), key);	
	    }
	  }

	public static class CCReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		Path OutputDirectory;
		protected FileSystem fs;
		private JobConf job;
		
		public void configure(JobConf job) {
		     try {
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
			
			Path nodeSummary = new Path(OutputDirectory, nodeID.toString());
			final SequenceFile.Writer writer = SequenceFile.createWriter(
		              fs, job, nodeSummary, Text.class, Text.class, CompressionType.NONE);
			
			while (keys.hasNext()){
				Text key = keys.next();
				writer.append(nodeID, key);					
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
	    final Path inDir = new Path(args[0]);
	    final Path outDir = new Path(args[1]);
	    FileInputFormat.setInputPaths(job, inDir);
	    FileOutputFormat.setOutputPath(job, outDir);
		
		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new ConflictCalculator(), args);
	    System.exit(res);
	}
}
