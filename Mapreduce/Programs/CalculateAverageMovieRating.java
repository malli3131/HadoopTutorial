package twok.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalculateAverageMovieRating {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws  
	 * @throws ClassNotFoundException 
	 * @throws InterIOExceptionruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "To Calculate Movie Average Rating");
		
		job.setJarByClass(CalculateAverageMovieRating.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		Text kword = new Text();
		LongWritable vword = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.split("\\t");
			if(parts.length == 4)
			{
				kword.set(parts[1]);
				vword.set(Long.valueOf(parts[2]));
				context.write(kword, vword);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, LongWritable, Text, FloatWritable>
	{
		FloatWritable vword = new FloatWritable();
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			float ratingAvg = 0.0f;
			long sum = 0;
			int counter = 0;
			for(LongWritable value : values)
			{
				counter++;
				sum = sum + value.get();
			}
			ratingAvg = (float) (sum / counter);
			vword.set(ratingAvg);
			context.write(key, vword);
		}
	}
}
