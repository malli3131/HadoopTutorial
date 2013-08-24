package twok.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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

public class TFIDF {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Finding the Term frequency and Invrse Document Frequency");
		
		job.setJarByClass(TFIDF.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
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
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		Text vword = new Text();
		Text kword = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] parts = line.trim().split("\\t");
			if(parts.length == 2)
			{
				StringTokenizer tokens = new StringTokenizer(parts[1]);
				while(tokens.hasMoreTokens())
				{
					kword.set(tokens.nextToken());
					vword.set(parts[0]);
					context.write(kword, vword);
				}
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		Map<String, Integer> tokens = null;
		Text vword = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			tokens = new HashMap<String, Integer>();
			int totalDocs = 180;
			for(Text value : values)
			{
				if(tokens.containsKey(value.toString()))
				{
					int count = tokens.get(value.toString());
					count++;
					tokens.put(value.toString(), count);
				}
				else
				{
					tokens.put(value.toString(), 1);
				}
			}
			int docAppears = tokens.size();
			float idf = (float) totalDocs / docAppears;
			Set<String> keys = tokens.keySet();
			String emitValue = "";
			for(String mykey : keys)
			{
				emitValue += mykey + ":" + tokens.get(mykey) + "," ;
			}
			emitValue = emitValue + "\tIDF Score " + idf;
			vword.set(emitValue);
			context.write(key, vword);
		}
	}
}