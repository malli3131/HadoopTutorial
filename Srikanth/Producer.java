package com.srikanth;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Producer {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "Producer Log Processing......");
		
		job.setJarByClass(Producer.class);
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
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
		String regex = "(^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}.\\d{3}).*\\s(\\d{13}\\+\\d{2,})\\].*(\\s\\w+[.]\\w+::\\w+[.]\\w+[.]\\w+[.]\\w+[.]\\w+).*";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = null;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			matcher = pattern.matcher(line);
			if(matcher.find())
			{
				emitKey.set(matcher.group(2).trim());
				emitValue.set("Pro:" + matcher.group(1).trim() + "\tPro:" + matcher.group(3).trim());
				context.write(emitKey, emitValue);
			}
		}
	}
}
