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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StockCount {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	//This is the method for defining the MapReduce Driver
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2)
		{
			System.out.println("Usage is: hadoop jar jarfile MainClass input output");
			System.exit(1);
		}
		
		Job job = new Job(conf, "Stocks Counting");
		
		job.setJarByClass(StockCount.class);
		
		//To set Mapper and Reduce classes
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		
		//Output Key-Value data types Type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//To inform input output Formats to MapReduce Program 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Inform input and output File or Directory locations
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Inform the job termination criteria
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		Text kword = new Text();
		LongWritable vword = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\\t");
			if(parts.length == 9)
			{
				kword.set(parts[1]);
				vword.set(1);
				context.write(kword, vword);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable value : values)
			{
				sum = sum + value.get();
			}
			context.write(key, new LongWritable(sum));
		}
	}
}
