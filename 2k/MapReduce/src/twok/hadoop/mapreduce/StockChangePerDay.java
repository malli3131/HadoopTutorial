package twok.hadoop.mapreduce;

import java.io.IOException;

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

public class StockChangePerDay {

	/**
	 * @param args
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
		
		Job job = new Job(conf, "Stocks change per day");
		
		job.setJarByClass(StockChangePerDay.class);
		
		//To set Mapper and Reduce classes
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//Output Key-Value data types Type
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		//To inform input output Formats to MapReduce Program 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Inform input and output File or Directory locations
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Inform the job termination criteria
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		Text kword = new Text();
		Text vword = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\\t");
			if(parts.length == 9)
			{
				kword.set(parts[1] + "\t" + parts[2]);
				vword.set(parts[3] + ":" + parts[6]);
				context.write(kword, vword);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, DoubleWritable>
	{
		DoubleWritable vword = new DoubleWritable();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			double change = 0.0;
			for(Text value : values)
			{
				String[] parts = value.toString().split("\\:");
				if(parts.length == 2)
				{
					change = Double.valueOf(parts[1]) - Double.valueOf(parts[0]);
				}
			}
			vword.set(change);
			context.write(key, vword);
		}
	}
}
