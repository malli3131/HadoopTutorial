package com.raj.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DataLoadMR {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	Configuration conf = new Configuration();
	if(args.length < 1)
	{
		System.out.println("This is Class for Loading the data from HDFS to HBase");
		System.out.println("Usage: hadoop jar jarfileName Mainclass -D mapred.reduce.task=2 hdfsInputFileLocation");
		System.exit(1);
	}
	String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	Job job = new Job(conf, "Find the volume of Stocks");
	
	job.setJarByClass(DataLoadMR.class);
	
	job.setMapperClass(MyMapper.class);
	TableMapReduceUtil.initTableReducerJob("stocks", MyReducer.class, job);
	
	job.setMapOutputKeyClass(LongWritable.class);
	job.setMapOutputValueClass(Text.class);
	
	
	job.setInputFormatClass(TextInputFormat.class);
	//job.setOutputFormatClass(TextOutputFormat.class);
	
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	//FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		Text kword = new Text();
		LongWritable vword = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
				context.write(key, value);
		}
	}
	public static class MyReducer extends TableReducer<LongWritable, Text, ImmutableBytesWritable>
	{
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Put data = null;
			for(Text value: values)
			{
				String[] parts = value.toString().split("\\t");
				if(parts.length == 9)
				{
					String myRow = parts[1] + ":" + parts[2];
					data = new Put(myRow.getBytes());
					data.add("bse".getBytes(), "open".getBytes(), parts[3].getBytes());
					data.add("bse".getBytes(), "high".getBytes(), parts[4].getBytes());
					data.add("bse".getBytes(), "low".getBytes(), parts[5].getBytes());
					data.add("bse".getBytes(), "close".getBytes(), parts[5].getBytes());
					data.add("bse".getBytes(), "volume".getBytes(), parts[7].getBytes());
					data.add("bse".getBytes(), "adj_close".getBytes(), parts[8].getBytes());
					context.write(null, data);
				}
			}
		}
	}
}

