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

public class SortedStockVolume {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */

	public static class MyMapper extends Mapper<Text, LongWritable, LongWritable, Text>{
		public void map(Text key, LongWritable value, Context context) throws InterruptedException, IOException
		{
				context.write(value, key);
		}
	}
	
	public static class MyReducer extends Reducer<LongWritable, Text, Text, LongWritable>
	{
		Text vword = new Text();
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String stock = "";
			for(Text value : values)
			{
				stock = value.toString();
			}
			vword.set(stock);
			context.write(vword, key);
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
		job.setJarByClass(SortedStockVolume.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}