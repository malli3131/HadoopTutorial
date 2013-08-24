package twok.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class DistributedCacheExample {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static final String filePath = "/cache/info";
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Distribute Cache Example");
	
	job.setJarByClass(DistributedCacheExample.class);
	
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(LongWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
	
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
			String dataParts[] = line.split("\\t");
			if(dataParts.length == 9)
			{
				kword.set(dataParts[1]);
				vword.set(Long.valueOf(dataParts[7]));
				context.write(kword, vword);
			}
		}
	}
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		Map<String, String> info = null;
		public void setup(Context context) throws IOException
        {
                loadKeys(context);
        }
        void loadKeys(Context context) throws IOException
        {
                FSDataInputStream in = null;
                BufferedReader br = null;
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(filePath);
                in = fs.open(path);
                br  = new BufferedReader(new InputStreamReader(in));
                info = new HashMap<String, String>();
                String line = "";
                while ( (line = br.readLine() )!= null) {
                String[] arr = line.split("\\,");
                if (arr.length == 2)
                	info.put(arr[0], arr[1]);
                }
                in.close();
        }
        Text kword = new Text();
		LongWritable vword = new LongWritable();
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			long sum = 0;
			for(LongWritable value : values)
			{
				sum = sum + value.get();
			}
			String myKey = key.toString() + "\t" + info.get(key.toString());
			kword.set(myKey);
			vword.set(sum);
			context.write(kword, vword);
		}
	}
}
