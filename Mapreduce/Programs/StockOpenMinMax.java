package twok.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class StocksOpenMinMax {
public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		Text kword = new Text();
		DoubleWritable vword = new DoubleWritable();
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			String line = value.toString();
			String[] parts = line.split("\\t");
			if(parts.length == 9)
			{
				String stockName = parts[1];
				double trade = Double.valueOf(3);
				kword.set(stockName);
				vword.set(trade);
				context.write(kword, vword);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, Text>
	{
		Text vword = new Text();
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
		{
			double min = Double.MAX_VALUE;
			double max = 0.0;
			for(DoubleWritable value : values)
			{
				double current = value.get();
				max = (max>current)?max:current;
				min = (min<current)?min:current;
			}
			vword.set("Min: " + min + "\tMax: " + max);
			context.write(key, vword);
		}
	} 
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
    
		Job job = new Job(conf, "Finding min and max of opening value of every stock");
		job.setJarByClass(StocksMinMaxOHLC.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
