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

public class StockVolumeAvgSum {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text kword = new Text();
		LongWritable vword = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			String line = value.toString();
			String[] parts = line.split("\\t");
			if(parts.length == 9)
			{
				String stockName = parts[1];
				long volume = Long.valueOf(parts[7]);
				kword.set(stockName);
				vword.set(volume);
				context.write(kword, vword);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, Text>
	{
		Text vword = new Text();
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			double avg = 0.0;
			int counter = 0;
			for(LongWritable value : values)
			{
				sum = sum + value.get();
				counter++;
			}
			avg = (double) sum / counter;
			vword.set("sum: " + sum + "\tAverage: " + avg);
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
		job.setJarByClass(StockVolumeAvgSum.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
