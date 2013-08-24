package twok.hadoop.mapreduce;

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

public class InvertedIndex {

	/**
	 * @param args
	 */
	public static class Mymap extends Mapper<LongWritable, Text,Text, Text>
	{
		Text emitKey = new Text();
		Text emitValue = new Text();
		String line = "";
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] dataParts = value.toString().trim().split("\\t");
			StringTokenizer token = new StringTokenizer(dataParts[1], " ");
			while(token.hasMoreTokens())
			{
				emitKey.set(token.nextToken());
				emitValue.set(dataParts[0]);
				context.write(emitKey, emitValue);
			}
		}
	}
	
	public static class Myreduce extends Reducer<Text, Text, Text, Text>
	{
		Text emitValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context  context) throws IOException, InterruptedException
		{
			StringBuilder myvalue = new StringBuilder(1024);
			for(Text value : values)
			{
				myvalue.append(value.toString()).append(",");
			}
			emitValue.set(myvalue.toString());
			context.write(key, emitValue);
		}
	}
	/** The following one is our Map Reduce Driver.
	* In this one, we are going to set all the Hadoop configuration and 
	* job management related parameters.
	* Some of them are:
	* What is our Map Reduce Job name
	* what is our Main Class for Map Reduce job
	* what is  our Mapper Class
	* what is our Reducer Class
	* what is our Combiner Class
	* what is our Partitioner Class
	* What is our Text Input Format class
	* what is our Text Output Format Class
	* What is our Map emitted output key class
	* what is our Map emitted output value class
	* what is our Overall output key class
	* what is our Overall output value class
	* what is the location of input data
	* what is the location of output data
	* 
	*/
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "To Build Inverted Index");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(Mymap.class);
		job.setReducerClass(Myreduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/patents/cite.csv"));
		FileOutputFormat.setOutputPath(job, new Path("/patents/index"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}