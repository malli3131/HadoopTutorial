Output: Both country and market are specified in the mapred and yarn site xmls resp. and market retrieved in Map task 
by using Mapper.Context and country retrieved in Reduce Task by using Reducer.Context


India: CA-bse	1470101300
India: CAB-bse	199893800
India: CACI-bse	84117700
India: CAE-bse	7102600
India: CAF-bse	58350100
India: CAG-bse	1089268300
India: CAH-bse	858600400
India: CAJ-bse	156958300
India: CAL-bse	1929194000
India: CAM-bse	1008806300
India: CAP-bse	12745800
India: CAS-bse	35253000
India: CASC-bse	18662100
India: CAT-bse	3230492600
India: CATO-bse	69685500
.
.
.
.
.


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

public class StockTotalVolume {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			String line = value.toString();
			String[] parts = line.split("\\t");
			if(parts.length == 9)
			{
				String stockName = parts[1];
				long volume = Long.valueOf(parts[7]);
				context.write(new Text(stockName + "-" + context.getConfiguration().get("market")), new LongWritable(volume));
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
			String country = context.getConfiguration().get("country");
			String record = country + ": " + key.toString(); 
			context.write(new Text(record), new LongWritable(sum));
		}
	} 
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Finding the stock Volume");
		job.setJarByClass(StockTotalVolume.class);
		
		job.setMapperClass(MyMapper.class);
		//job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
