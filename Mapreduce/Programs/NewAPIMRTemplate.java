package twok.hadoop.mapreduce;

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

public class MRTemplate {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void setup(Context context) throws IOException{
			//Initialize all the variables which are used in map method
		}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			//Map side Business Logic i.e., Data Transformations
		}
		public void cleanup(Context context) throws IOException{
			//Close all the variables which are used in map method
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, LongWritable>
	{
		public void setup(Context context) throws IOException{
			//Initialize all the variables which are used in reduce method
		}
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			//Reduce side Business Logic i.e., Data Aggregations
		}
		public void cleanup(Context context) throws IOException{
			//Close all the variables which are used in reduce method
		}
	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Name of the MapReduce Job");
		
		job.setJarByClass(MRTemplate.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
}
