package twok.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SequenceMapReduce {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */

	public static class MyMapper extends Mapper<Text, LongWritable, Text, LongWritable>{
		public void map(Text key, LongWritable value, Context context) throws InterruptedException, IOException
		{
				context.write(key, value);
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		LongWritable vword = new LongWritable();
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable value : values)
			{
				sum = sum + value.get();
			}
			vword.set(sum);
			context.write(key, vword);
		}
	} 
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.out.println("Usage is: hadoop jar jarfile MainClass input output");
			System.exit(1);
		}
		Job job = new Job(conf, "Finding sum of the the stock Volume");
		job.setJarByClass(SequenceMapReduce.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
