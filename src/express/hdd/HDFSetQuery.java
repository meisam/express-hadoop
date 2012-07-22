package express.hdd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
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
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSetQuery extends Configured implements Tool {
	
	public static class HDFSetQueryMapper extends MapReduceBase 
    implements Mapper<Text, Text, Text, Text> {
		
		ArrayList<int[]> offsets;
		ArrayList<int[]> lengths;
		private static int dimension;
		private static int [] dataSize;
		
		@SuppressWarnings("unused")
		private JobConf job;
		
		public void configure(JobConf job) {
			try {
				dataSize = Tools.StringArray2IntArray(job.get("DataSize").toString().split(","));
				offsets = Tools.str2IntArrayList(job.get("ChunkOffsets"));
				lengths = Tools.str2IntArrayList(job.get("ChunkLengths"));
				dimension = dataSize.length;
				this.job=job;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	    public void map(Text key, Text value,
              OutputCollector<Text, Text> fout,
              Reporter reporter) throws IOException {
	    	
	    	int[] coffset = {}; //mapper input offset
	    	int[] clength = {}; //mapper input size
	    	try {
				Pair<int[], int[]> keyPair = Tools.text2Pair(key);
				coffset = keyPair.getLeft();
				clength = keyPair.getRight();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace(); 
			}
			
			int[] aboffset = {};
			for (int i=0; i<offsets.size(); i++){
				Pair<int[], int[]> outputPair = HyperRectangleData.getHyperRectangleIntersection(coffset,
						clength, offsets.get(i), lengths.get(i), dimension);
				if (outputPair == null)
					continue;
				aboffset = outputPair.getLeft();
				int[] roffset = HyperRectangleData.getRelativeOffset(coffset, aboffset, dimension);
				int[] rlength = outputPair.getRight(); // relative length = absolute length
				byte[] buffer = new byte[HyperRectangleData.getVolume(rlength)];
				HyperRectangleData.getChunkOfHighDimData(value.getBytes(), clength, dimension, buffer, roffset,rlength);
							
				try {
					Text naturalKey = Tools.pair2Text(offsets.get(i), lengths.get(i) );
					Text secondaryKey = Tools.pair2Text(aboffset, rlength);
					Text fvalue = Tools.pair2Text(aboffset, rlength); //final value = relative key + value
					fvalue.append(buffer, 0, buffer.length); //fvalue = fvalue + buffer
					fout.collect(HDFIntermediateKey.merge(naturalKey, secondaryKey), fvalue);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
	    }
	  }
	
	public static class HDFSetQueryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		protected FileSystem fs;
		private JobConf job;

		private static Path OutputDir;
		private static boolean isWriter;
		private static int waitSecs;
		
		public void configure(JobConf job) {
			try {
				fs = FileSystem.get(job);
				this.job = job;
				OutputDir=  new Path(job.get("OutputDirectory").toString());
				isWriter = job.getBoolean("hdf.reduce.write", false);
				waitSecs = Integer.parseInt(job.get("hdf.reduce.wait"));			     
		     } catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> fout, Reporter arg3)
				throws IOException {
		
			try {
				Pair<int[], int[]> outputChunk = Tools.text2Pair(HDFIntermediateKey.getNaturalKey(key));
				
				Path reducerFile = new Path(OutputDir, Tools.pair2Text(outputChunk).toString());
				final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
			    			, reducerFile, key.getClass(), Text.class, CompressionType.NONE);
				while (values.hasNext()){
					Text value = values.next();
					int keyEndOffset = value.find("]", value.find("]")+1); //find the second ']' since key looks like[];[]
					Text outputKey = new Text(Arrays.copyOfRange(value.getBytes(), 0, keyEndOffset+1));
					Text outputValue = new Text(Arrays.copyOfRange(value.getBytes(), keyEndOffset+1, value.getLength()));
						
					if (isWriter) {
						writer.append(outputKey, outputValue);	
					}
				}
				if (waitSecs > 0)
						Thread.sleep(waitSecs * 1000);
				writer.close();		
			

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace(); 
			}
		}
		
	}



	@Override
	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		final FileSystem fs = FileSystem.get(job);
		DistributedFileSystem dfs = (DistributedFileSystem)fs;
		int nodeAmount = dfs.getDataNodeStats().length; 
		
		job.set("DataSize", args[0]);
		job.set("PartitionOffset", args[1]);
		job.set("PartitionRecordLength", args[2]);
		job.set("PartitionSize", args[3]);
		job.set("ChunkOffsets", args[4]);
		job.set("ChunkLengths", args[5]);
		job.set("InputDirectory", args[6]);
		job.set("OutputDirectory", args[7]);
		job.set("NumberOfNodes", Integer.toString(nodeAmount));
		job.setBoolean("isHighDimensionData", new Boolean(args[8]));
		
		ArrayList<int[]> offsets = Tools.str2IntArrayList(job.get("ChunkOffsets"));
		ArrayList<int[]> lengths = Tools.str2IntArrayList(job.get("ChunkLengths"));
		
		job.setJobName("HDFSetQuery");
	    job.setJarByClass(HDFSetQuery.class);
	    job.setMapperClass(HDFSetQueryMapper.class);
	    job.setReducerClass(HDFSetQueryReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormat(NullOutputFormat.class);// Each reducer outputs to a single file
	    job.setPartitionerClass(HDFSetQueryPartitioner.class);
	    
	    if (job.getBoolean("hdf.reduce.bypass", false))
	    	job.setNumReduceTasks(0);
	    else if (job.getBoolean("hdf.reduce.one", false))
	    	job.setNumReduceTasks(1);
	    else
	    	job.setNumReduceTasks(offsets.size());
	    
	    HyperRectangleData origData = new HyperRectangleData(
				Tools.getHDFVectorFromConf(job, "DataSize"),
				Tools.getHDFVectorFromConf(job, "PartitionOffset"), 
				Tools.vectorDotX(Tools.getHDFVectorFromConf(job, "PartitionSize"),
						Tools.getHDFVectorFromConf(job, "PartitionRecordLength")),
				Tools.getHDFVectorFromConf(job, "DataSize").length);
	    
	    job.setInputFormat(SequenceFileAsTextInputFormat.class);
	    job.setSpeculativeExecution(false);
	    job.setOutputKeyComparatorClass(HDFOutputKeyComparator.class);
	    job.setOutputValueGroupingComparator(HDFoutputvaluegroupingcomparator.class);
	    
	    final Path inDir = new Path(args[6]);
	    final Path outDir = new Path(args[7]);
	    FileInputFormat.setInputPaths(job, inDir);
	    FileOutputFormat.setOutputPath(job, outDir);
	    
	    addInputSplits(job, inDir, origData, offsets, lengths);
		
		JobClient.runJob(job);    
	    
		return 0;
	}
	
	private static void addInputSplits(JobConf job, Path in, HyperRectangleData rec, 
			ArrayList<int[]> offsets, ArrayList<int[]> lengths) {
		HashSet<Integer> chunkIDPool = new HashSet<Integer>();
		for (int i=0; i<offsets.size(); i++){
			System.out.printf("has chunk %s\n", Arrays.toString(offsets.get(i)) + " | " + Arrays.toString(lengths.get(i)));
			int[] ids = rec.getOverlappedChunks(offsets.get(i), lengths.get(i));
			for (int j=0; j<ids.length; j++)
				chunkIDPool.add(ids[j]);
		}
		
		Iterator<Integer> idPool = chunkIDPool.iterator();
		
		while (idPool.hasNext()){
			Path file = new Path(in, idPool.next().toString());
			//System.out.printf("has chunk %s", file);
			FileInputFormat.addInputPath(job, file);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new JobConf(), new HDFSetQuery(), args);
	    System.exit(res);
	}
	
}
