/*
This used to process the twitter data which was crawled by Flume.
*/
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class CombineTextInputFormatExample {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		String userArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Twitter Data Cleaner");
		
		job.setJarByClass(CombineTextInputFormatExample.class);
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(CombineTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(userArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1);
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		LongWritable emitKey = new LongWritable();
		Text emitValue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String record = value.toString();
			JSONObject json = null;
			try{
				json = (JSONObject) new JSONParser().parse(record);
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
			long id = (Long) json.get("id");
			String tweet = (String) json.get("text");
			emitKey.set(id);
			emitValue.set(tweet);
			context.write(emitKey, emitValue);
		}
	}
}
