package express.util;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BooleanWritable;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import express.hdd.HDFGen;
import express.hdd.HDFGen.HDFGenMapper;

public class MakePipeFile extends Configured implements Tool {
	static private final Path TMP_DIR = new Path("MICRO_HDF_TMP");
	
	public static class MakePipeFileMapper extends MapReduceBase 
    implements Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {
		
		protected FileSystem fs;

		private static Path OutputDir;
		private JobConf job;
		
		public void configure(JobConf job) {
		     try {
			     OutputDir=  new Path(job.get("OutputDirectory").toString());
			     this.job=job; 
			    		 
			     fs = FileSystem.get(job);
		     } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		
	    public void map(LongWritable offset,
              LongWritable size,
              OutputCollector<BooleanWritable, LongWritable> fout,
              Reporter reporter) throws IOException {

	    	Path pipeFile = new Path(OutputDir, offset.toString());
	    	final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
	    			, pipeFile, Text.class, Text.class, CompressionType.NONE);
	    	writer.close();
	    }
	  }
	
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		job.set("NumberOfPipes", args[0]);
		job.set("OutputDirectory", args[1]);
		
		//FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.setJobName("MakePipeFile");
	    job.setJarByClass(MakePipeFile.class);
	    job.setNumReduceTasks(0);
	    job.setMapperClass(MakePipeFileMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setInputFormat(SequenceFileInputFormat.class);
	    job.setSpeculativeExecution(false);
	    //job.setOutputFormat((Class<? extends OutputFormat>) NullOutputFormat.class);
	    
	    final Path inDir = new Path(TMP_DIR, "in");
	    final Path outDir = new Path(TMP_DIR, "out");
	    FileInputFormat.setInputPaths(job, inDir);
	    FileOutputFormat.setOutputPath(job, outDir);
	    final FileSystem fs = FileSystem.get(job);

		DistributedFileSystem dfs = (DistributedFileSystem)fs;
		dfs.refreshNodes();
		int nodeAmount = dfs.getDataNodeStats().length; 
		job.set("NumberOfNodes", Integer.toString(nodeAmount));
		
	    if (!fs.mkdirs(inDir)) {
	        throw new IOException("Cannot create input directory " + inDir);
	    }
	    
	    try {
	        //generate an input file for each map task
	    	int numberOfPipes = Integer.parseInt(job.get("NumberOfPipes"));
	    	System.out.println("Number of Pipes to Create: " + numberOfPipes);
	        for(int i=0; i < numberOfPipes; ++i) {
	          final Path file = new Path(inDir, "part"+i);
	          final LongWritable offset = new LongWritable(i);
	          final LongWritable size = new LongWritable(1);
	          final SequenceFile.Writer writer = SequenceFile.createWriter(
	              fs, job, file,
	              LongWritable.class, LongWritable.class, CompressionType.NONE);
	          try {
	            writer.append(offset, size);
	          } finally {
	            writer.close();
	          }
	          System.out.println("Wrote input for Map #"+i);
	        }
	        
		JobClient.runJob(job);
	    } finally {
	        fs.delete(TMP_DIR, true);
	    }
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new JobConf(), new HDFGen(), args);
	    System.exit(res);
	}
}
