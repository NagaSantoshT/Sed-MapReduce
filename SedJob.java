import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SedJob implements Tool 
{
	private Configuration conf;

	@Override
	public Configuration getConf() 
	{
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) 
	{
		// TODO Auto-generated method stub
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		// TODO Auto-generated method stub
		Job sedJob = new Job(getConf());
		
		sedJob.setJobName("Grep Job");
		sedJob.setJarByClass(this.getClass());
		
		sedJob.setMapperClass(SedMapper.class);
		sedJob.setMapOutputKeyClass(Text.class);
		sedJob.setMapOutputValueClass(NullWritable.class);
		
		sedJob.setNumReduceTasks(0);
		
		sedJob.setOutputKeyClass(Text.class);
		sedJob.setOutputValueClass(NullWritable.class);
		
		sedJob.setInputFormatClass(TextInputFormat.class);
		sedJob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(sedJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sedJob, new Path(args[1]));
		
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		return sedJob.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new SedJob(), args);
	}
}
