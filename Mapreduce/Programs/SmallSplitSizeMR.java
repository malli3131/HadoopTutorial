package com.impetus.data.generate;

import java.io.IOException;
import java.util.Random;

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

public class SmallSplitSizeMR {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		int rowNums = 0;
		public void setup(Context context)
		{
			rowNums = context.getConfiguration().getInt("rows", 80000);
		}
		Text emitKey = new Text();
		Text emitValue = new Text();
		StringBuilder keyRecord = null;
		StringBuilder valueRecord = null;
		Random myRan = new Random();
		public void map(LongWritable key, Text value, Context context)
				throws InterruptedException, IOException { 
			String columns[] = value.toString().split("\\t");
			if(columns.length == 9)
			{
				for(int i=0; i<rowNums; i++)
				{
					long volume = (long) (Long.valueOf(columns[7]) * myRan.nextDouble());
					keyRecord = new StringBuilder(200);
					valueRecord = new StringBuilder(200);
					
					keyRecord.append(columns[0]);
					keyRecord.append("\t");
					keyRecord.append(columns[1]);
					keyRecord.append(i);
					
					valueRecord.append(columns[2]);
					valueRecord.append("\t");
					valueRecord.append(columns[3]);
					valueRecord.append("\t");
					valueRecord.append(columns[4]);
					valueRecord.append("\t");
					valueRecord.append(columns[5]);
					valueRecord.append("\t");
					valueRecord.append(columns[6]);
					valueRecord.append("\t");
					valueRecord.append(volume);
					valueRecord.append("\t");
					valueRecord.append(columns[8]);
					
					emitKey.set(keyRecord.toString());
					emitValue.set(valueRecord.toString());
					
					context.write(emitKey, emitValue);
				}
			}
		}
	}
	@SuppressWarnings("deprecation")
	public static void main(String args[]) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String userArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(userArgs.length < 3)
		{
			System.out.println("Usage: hadoop jar jarfilename mainclass input output rows");
			System.exit(1);
		}
		conf.setInt("mapred.max.split.size", 1024 * 256);
		conf.setInt("mapred.tasktracker.map.tasks.maximum", 4);
		conf.setInt("rows", Integer.valueOf(userArgs[2]));
		
		Job job = new Job(conf, "Using small split size than split size as hdfs block size");
		job.setJarByClass(SmallSplitSizeMR.class);

		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
