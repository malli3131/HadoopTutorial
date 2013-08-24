package twok.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyChainMapper {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();

		JobConf job = new JobConf(conf);
		job.setJobName("Chaining MapReduce");
		job.setJarByClass(MyChainMapper.class);

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		Configuration mapperOneConf = new Configuration(false);
		JobConf mapJob1 = new JobConf(mapperOneConf);
		ChainMapper.addMapper(job, MyMapperOne.class, LongWritable.class, Text.class, Text.class, LongWritable.class, true, mapJob1);

		Configuration mapperSecConf = new Configuration(false);
		JobConf mapJob2 = new JobConf(mapperSecConf);
		ChainMapper.addMapper(job, MyMapperSecond.class, Text.class, LongWritable.class, Text.class, LongWritable.class, true, mapJob2);

		job.setReducerClass(MyReducer.class);

		Configuration mapperThreeConf = new Configuration(false);
		JobConf mapJob3 = new JobConf(mapperThreeConf);
		ChainMapper.addMapper(job, MyMapperThree.class, Text.class, LongWritable.class, Text.class, LongWritable.class, true, mapJob3);

		JobClient.runJob(job);
	}

	public static class MyMapperOne extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> collect, Reporter reporter)
						throws IOException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);
			while(st.hasMoreTokens())
			{
				collect.collect(new Text(st.nextToken()), new LongWritable(1));
			}
		}
	}

	public static class MyMapperSecond extends MapReduceBase implements Mapper<Text, LongWritable, Text, LongWritable>
	{
		List<String> stopwords = null;
		public void configure(JobConf conf)
		{
			conf = new JobConf();
			stopwords = new ArrayList<String>();
			stopwords.add("and");
			stopwords.add("is");
			stopwords.add("am");
			stopwords.add("at");
			stopwords.add("in");
			stopwords.add("after");
			stopwords.add("did");
			stopwords.add("will");
		}
		public void map(Text key, LongWritable value,
				OutputCollector<Text, LongWritable> collect, Reporter reporter)
						throws IOException {
			String myKey = key.toString();
			if(!stopwords.contains(myKey))
			{
				collect.collect(key, value);
			}
		}
	}

	public static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> collect, Reporter reporter)
						throws IOException {
			long sum = 0;
			while(values.hasNext())
			{
				sum = sum + values.next().get();
			}
			collect.collect(key, new LongWritable(sum));
		}
	}
	public static class MyMapperThree extends MapReduceBase implements Mapper<Text, LongWritable, Text, LongWritable>
	{
		public void map(Text key, LongWritable value,
				OutputCollector<Text, LongWritable> collect, Reporter reporter)
						throws IOException {
			long val = value.get() * 10;
			collect.collect(key, new LongWritable(val));
		}
	}
}