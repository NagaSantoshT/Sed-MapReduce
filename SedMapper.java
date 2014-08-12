import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SedMapper extends Mapper<LongWritable, Text, Text, NullWritable>
{
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException 
	{
		String line = value.toString();
		
		if (line.contains("I")) 
		{
			String replaceLine = line.replaceAll("I", "i");
			context.write(new Text(replaceLine), NullWritable.get());
		}
	}
}
