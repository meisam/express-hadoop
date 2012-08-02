package express.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AppendSth2File extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf job = (JobConf) getConf();
		FileSystem fs = FileSystem.get(job);
		String file = args[0];
		
		Path fileFile = new Path(file);

		final SequenceFile.Writer writer = SequenceFile.createWriter(fs, job
    			, fileFile, Text.class, Text.class, CompressionType.NONE);
		Text dummy = new Text("xyz");
		writer.append(dummy, dummy);
		writer.close();
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new JobConf(), new AppendSth2File(), args);
	    System.exit(res);
	}


}
