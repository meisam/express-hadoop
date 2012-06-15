package express.hdd;

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

/**
 * Generate the HDF-MR input data set.
 * The user specifies the size of the data, offset of one record, length of the record 
 * ,#records in one partition, and the output directory and this class runs a map/reduce program to generate the data.
 * The format of the data is:
 * <ul>
 * <li>(x1,x2,...) (l1,l2,...) (data of a record size) \r \n
 * </ul>
 *
 * <p>
 * To run the program: 
 * <b>bin/hadoop jar express-hadoop.jar hdf.test.HDFGen [dataSize] [partitionOffset] [recordSize] [partitionSize] [outDir]</b>
 * e.g.
 * <b>bin/hadoop jar express-hadoop.jar hdf.test.HDFGen 32,64,128 0,0,0 4,8,8 4,4,4 hdf-test</b>
 */

public class HDFGen extends Configured implements Tool{
 
	static private final Path TMP_DIR = new Path("MICRO_HDF_TMP");
	
	public static class HDFGenMapper extends MapReduceBase 
      implements Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {
		
		protected FileSystem fs;

		private static int [] dataSize;
		@SuppressWarnings("unused")
		private static int [] roffset;
		private static int [] rlength;
		private static int [] partitionSize;
		private static int dimension;
		private static int recordNumInPartition;
		private static int NumOfPartitions;
		private static Path OutputDir;
		private static int nodeAmount;
		private static int TICKS = 16000;
		private JobConf job;
		
  
		private int[] StringArray2IntArray(String[] sarray) throws Exception {
			if (sarray != null) {
				int intarray[] = new int[sarray.length];
				for (int i = 0; i < sarray.length; i++) {
						intarray[i] = Integer.parseInt(sarray[i]);
				}
				return intarray;
			}
			return null;
		}
		
		public void configure(JobConf job) {
		     try {
		    	 dataSize = StringArray2IntArray(job.get("DataSize").toString().split(","));
			     roffset = StringArray2IntArray(job.get("PartitionOffset").toString().split(","));
			     rlength = StringArray2IntArray(job.get("PartitionRecordLength").toString().split(","));
			     partitionSize = StringArray2IntArray(job.get("PartitionSize").toString().split(","));
			     OutputDir=  new Path(job.get("OutputDirectory").toString());
			     dimension = dataSize.length;
			     nodeAmount = Integer.parseInt(job.get("NumberOfNodes"));
			     this.job=job;
			     
			     recordNumInPartition=1;
			     for (int i=0; i<dimension; i++)
			    	 recordNumInPartition = recordNumInPartition * partitionSize[i];
			     
			     NumOfPartitions = 1;
			     for (int i=0; i<dimension; i++)
			    	 NumOfPartitions = NumOfPartitions * dataSize[i]/(rlength[i] * partitionSize[i]);  
			    		 
			     fs = FileSystem.get(job);
		     } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		private byte[] getkey(int recordOffset, int partitionOffset){
			int[] offset = new int[dimension];
			int[] pincr = new int[dimension];
			int[] rincr = new int[dimension];
			
			for (int i = 0, pstride = 1; i < dimension; i++){
				pincr[i] =  (partitionOffset/pstride)%(dataSize[i]/(partitionSize[i]*rlength[i]));
				pstride = pstride * (dataSize[i]/(partitionSize[i]*rlength[i]));
			}
			
			for (int i = 0, rstride = 1; i < dimension; i++){
				rincr[i] =  (recordOffset/rstride)%(partitionSize[i]);
				rstride = rstride * (partitionSize[i]);
			}
			
			for (int d = 0; d < dimension; d++){
				offset[d] = pincr[d] * partitionSize[d]*rlength[d] + rincr[d]*rlength[d];//offset	
			}
			//System.out.printf("recordOffset = %d, partitionOffset = %d, %s\n",
					//recordOffset, partitionOffset,Arrays.toString(offset));
			return (Arrays.toString(offset)+ ";" + Arrays.toString(rlength)+ "\t").getBytes();
		}
		
	    public void map(LongWritable offset,
                LongWritable size,
                OutputCollector<BooleanWritable, LongWritable> fout,
                Reporter reporter) throws IOException {
	    	
	    	int recordSize = 1;
	    	for (int i=0; i < rlength.length; i++) {
	    		recordSize = recordSize * rlength[i];
	    	}
	    	
	    	//System.out.printf("recordSize = %d, recordNumInPartition = %s\n", recordSize, recordNumInPartition);
	    	/*byte[] buffer = new byte[recordSize];
	    	for(int i=0; i < recordSize; i++)
	            buffer[i] = (byte)('0' + i % 20);*/
	    	
	    	byte[] buffer = new byte[recordSize];
	    	for(int i=0; i < recordSize; i++)
	            buffer[i] = (byte)('0' + i % 20);
	    	Text key = new Text();
	    	Text value = new Text();
	    	
	    	/*try{
	    		// Create file 
	    		FileWriter fstream = new FileWriter("/tmp/" + offset.toString() + ".hdf.test");
	    		BufferedWriter out = new BufferedWriter(fstream);
	    		out.write(key);
	    		out.write("\t");
	    		out.write("Hello Java");
	    		out.write(buffer);
	    		out.write("\n");
	    		//Close the output stream
	    		out.close();
	    	}catch (Exception e){//Catch exception if any
	    		System.err.println("Error: " + e.getMessage());
	    	}*/	    	
	    	
	    	for (int i = Integer.valueOf(offset.toString()); i < NumOfPartitions; i = i + nodeAmount) {
	    		Path partitionFile = new Path(OutputDir, Integer.toString(i));
		    	final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
		    			, partitionFile, key.getClass(), value.getClass(), CompressionType.NONE);
	    		try {
	    			for (int j = 0; j < recordNumInPartition; j++) {
		    			key.set(getkey(j, i));
		    			value.set(buffer);		    			
		    			writer.append(key, value);		    	
	    			}
	    		} finally {
	    			writer.close();
	    			synchronized(this){
	    				try {
							this.wait(TICKS);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
	    			}
	    		}
		    
	    	}
	    }
	  }
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		job.set("DataSize", args[0]);
		job.set("PartitionOffset", args[1]);
		job.set("PartitionRecordLength", args[2]);
		job.set("PartitionSize", args[3]);
		job.set("OutputDirectory", args[4]);
		
		//FileOutputFormat.setOutputPath(job, new Path(args[4]));
		job.setJobName("HDFGen");
	    job.setJarByClass(HDFGen.class);
	    job.setNumReduceTasks(0);
	    job.setMapperClass(HDFGenMapper.class);
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
	    	System.out.println("Number of Nodes Available: " + nodeAmount);
	        for(int i=0; i < nodeAmount; ++i) {
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
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new JobConf(), new HDFGen(), args);
	    System.exit(res);
	}
}
