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


public class OldAPIStocksTotalVolume {
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
		Text kword = new Text();
		IntWritable vword = new IntWritable();
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> collector, Reporter reporter)
				throws IOException {
				String line = value.toString();
				String parts[] = line.split("\\t");
				if(parts.length == 9)
				{
					kword.set(parts[1]);
					vword.set(Integer.valueOf(parts[7]));
					collector.collect(kword, vword);
				}
		}
	}
	
	public static class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, LongWritable>
	{
		LongWritable vword = new LongWritable();
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, LongWritable> collector, Reporter reporter)
				throws IOException {
			long sum = 0L;
			while(values.hasNext())
			{
				sum = sum + values.next().get();
			}
			vword.set(sum);
			collector.collect(key, vword);
		}

	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		JobConf job = new JobConf(conf);
		job.setJobName("Finding the Stocks Volume using Old MapReduce API");
		
		job.setJarByClass(OldAPIStocksTotalVolume.class);
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
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