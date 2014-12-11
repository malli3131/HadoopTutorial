package com.hadoop.unit;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class UserDefinedCounters {
	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */
	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		Text emitKey = new Text();
		IntWritable emitvalue = new IntWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String cols[] = record.split(":");
			if (cols.length == 5 && cols[1].equals("1")) {
				emitKey.set(cols[4]);
				emitvalue.set(1);
				context.write(emitKey, emitvalue);
			}
			else{
				context.getCounter(BadRecords.myCounter).increment(1);
			}
		}
    //Use enum to declare the counter names and use these counters inside in map method.....
		static enum BadRecords {
			myCounter;
		};
	}

	public static class MyReducer extends
			Reducer<Text, IntWritable, Text, LongWritable> {
		LongWritable emitValue = new LongWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			emitValue.set(sum);
			context.write(key, emitValue);
		}
	}

	public static void main(String args[]) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String userArgs[] = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "SUser defined Counters.....");
		job.setJarByClass(UserDefinedCounters.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(userArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
