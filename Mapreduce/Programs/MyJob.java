package hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class MyJob {

	/**
	 * @author malli 
	 * @param args
	 */
	//Mapper Class for implementing Map level business Logic
	public static class MyMapClass extends Mapper<LongWritable, Text, Text, Text>
	{
		String line = "";
		Text emitKey = new Text();
		Text emitValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			line = value.toString().trim();
			String[] lineParts = line.split("\\t");
			StringTokenizer tokens = new StringTokenizer(lineParts[1], " ");
			while(tokens.hasMoreTokens())
			{
				emitValue.set(lineParts[0]);
				emitKey.set(tokens.nextToken());
				context.write(emitKey, emitValue);
			}
		}
	}
	
	//Reducer class for Implemennting Reduce Level business logic
	public static class MyReduceClass extends Reducer<Text, Text, Text, Text>
	{
		Text emitValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			StringBuilder myvalue = new StringBuilder(1024);
			for(Text value : values)
			{
				myvalue.append(value.toString().trim()).append("  ");
			}
			emitValue.set(myvalue.toString());
			context.write(key, emitValue);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "To Build Inverted Index");
		
		job.setJarByClass(MyJob.class);
		
		job.setMapperClass(MyMapClass.class);
		job.setReducerClass(MyReduceClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
