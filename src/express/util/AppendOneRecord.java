package express.util;

import java.util.Arrays;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import express.hdd.Tools;

public class AppendOneRecord extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		FileSystem fs = FileSystem.get(job);
		String file = args[0];
		job.set("RecordOffset", args[1]);
		job.set("RecordLength", args[2]);
		
		int roffset[] = Tools.getHDFVectorFromConf(job, "RecordOffset");
		int rlength[] = Tools.getHDFVectorFromConf(job, "RecordLength");
		int rsize = Tools.cumulativeProduction(rlength);
		byte[] buffer = new byte[rsize];
		for(int i=0; i < rsize; i++)
            buffer[i] = (byte)('0' + i % 20);
		
		Path filepath = new Path(file);
		final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
    			, filepath, Text.class, Text.class, CompressionType.NONE);
		
		writer.append(Tools.pair2Text(roffset, rlength), new Text(buffer));
		writer.close();
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new JobConf(), new AppendOneRecord(), args);
	    System.exit(res);
	}


}
