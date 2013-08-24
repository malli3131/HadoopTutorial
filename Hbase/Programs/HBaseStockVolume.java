package com.raj.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class HBaseStockVolume {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	Configuration conf = new Configuration();
	System.out.println("Usage: hadoop jar jarfileName Mainclass -D mapred.reduce.task=2");
	//String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
	Job job = new Job(conf, "Find the volume of Stocks");
	
	job.setJarByClass(HBaseStockVolume.class);
	
	Scan scan = new Scan();
	scan.addColumn("nyse".getBytes(), "volume".getBytes());
	TableMapReduceUtil.initTableMapperJob("stocks", scan, MyMapper.class, Text.class, LongWritable.class, job);
	TableMapReduceUtil.initTableReducerJob("reports", MyReducer.class, job);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	
	
	//job.setInputFormatClass(TextInputFormat.class);
	//job.setOutputFormatClass(TextOutputFormat.class);
	
	//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	//FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
	
	
	public static class MyMapper extends TableMapper<Text, LongWritable>
	{
		Text kword = new Text();
		LongWritable vword = new LongWritable();
		public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException
		{
			String rowkey = Bytes.toString(value.getRow());
			long volume = Long.valueOf(Bytes.toString(value.getValue("nyse".getBytes(), "volume".getBytes())));
			String parts[] = rowkey.split("\\:");
			kword.set(parts[0]);
			vword.set(volume);
			context.write(kword, vword);
		}
	}
	public static class MyReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable>
	{
		public void setup(Context context) throws IOException
		{
			HTable mytable = new HTable(context.getConfiguration(), "masterdata");
		}
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			Put data = null;
			long sum = 0L;
			for(LongWritable value: values)
			{
				sum = sum + value.get();
			}
			data = new Put(key.toString().getBytes());
			data.add("summary".getBytes(), "agg_volume".getBytes(), String.valueOf(sum).getBytes());
			context.write(null, data);
		}
	}
}