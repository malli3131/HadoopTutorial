package com.srikanth;

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

public class MessageLog {

	/**
	 * This is used to join the consumer and producer fields based on messageid.......
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "Joining the Consumer and Producer Message Logs.....");
		
		job.setJarByClass(MessageLog.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		Text emitKey = new Text();
		Text emitValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String parts[] = value.toString().split("\\t");
			String messageid = parts[0];
			String others = "";
			emitKey.set(messageid);
			int k = 0;
			for(int i =1; i<parts.length; i++)
			{
				if(k == 0)
				{
					others = parts[i];
					k = 1;
				}
				else
				{
					others = others + "\t" + parts[i];
				}
			}
			emitValue.set(others);
			context.write(emitKey, emitValue);
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		Text emitValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String message = "";
			int i = 0;
			for(Text value : values)
			{
				if(i == 0)
				{
					message = value.toString();
					i = 1;
				}
				else
				{
					message = message + "\t" + value.toString();
				}
			}
			emitValue.set(message);
			context.write(key, emitValue);
		}
	}
}
