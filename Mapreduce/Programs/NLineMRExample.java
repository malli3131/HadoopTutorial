import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NLineMRExample {

	/**
	 * @param args
	 */
	public static class MapClass extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(key, value);
		}
	}

	public static void main(String[] arg) throws IOException, InterruptedException, ClassNotFoundException 
	{
		Configuration conf = new Configuration();

		String[] args = new GenericOptionsParser(arg).getRemainingArgs();
		
		Job job = new Job(conf, "NLine Input Format Example");
		job.setJarByClass(NLineMRExample.class);
		
		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);
	
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 10);
		
		job.setInputFormatClass(NLineInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		NLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
