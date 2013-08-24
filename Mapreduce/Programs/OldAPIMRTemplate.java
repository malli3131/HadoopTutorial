package twok.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class OldAPIMRTemplate {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> collector, Reporter reporter)
				throws IOException {
			//Map side Business Logic
		}
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable>
	{

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			//Reduce side Business Logic
		}

	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		JobConf job = new JobConf(conf);
		job.setJobName("Name of the Job");
		
		job.setJarByClass(OldAPIMRTemplate.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		JobClient.runJob(job);
	}
}