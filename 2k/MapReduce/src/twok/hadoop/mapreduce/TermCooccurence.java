package twok.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

public class TermCooccurence {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Finding Term Cooccurence");
		
		job.setJarByClass(TermCooccurence.class);
		
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
		Text kword = new Text();
		Text vword = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String content = value.toString();
			String parts[] = content.split("\\t");
			String mycontent = parts[1].replaceAll("[^a-zA-Z0-9\\s]", "");
			String tokens[] = mycontent.split(" ");
			Set<String> words = new HashSet<String>();
			for(String token: tokens)
			{
				words.add(token);
			}
			for(int i=0; i<words.size(); i++)
			{
				String emitValue = "";
				//emitValue = emitValue + parts[0] + ":";
				for(int j=i+1; j<words.size(); j++)
				{
					emitValue += tokens[j] + ",";
				}
				kword.set(tokens[i]);
				vword.set(emitValue);
				context.write(kword, vword);
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		Text vword = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			Set<String> uniqWords = new HashSet<String>();
			for(Text value: values)
			{
				String parts[] = value.toString().split("\\,");
				for(String part : parts)
				{
					uniqWords.add(part);
				}
			}
			String mywords = "";
			for(String word : uniqWords)
			{
				mywords = mywords + ", " + word;
			}
			vword.set(mywords);
			context.write(key, vword);
		}
	}
}
