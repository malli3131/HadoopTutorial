package twok.hadoop.mapreduce;

import java.io.IOException;

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

public class MapReduceProfiler {

/**
* @param args
* @author Nagamallikarjuna
* @throws IOException
*/

public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
{
String line = value.toString();
String[] parts = line.split("\\t");
if(parts.length == 9)
{
String stockName = parts[1];
long volume = Long.valueOf(parts[7]);
context.write(new Text(stockName), new LongWritable(volume));
}
}
}

public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
{
long sum = 0;
for(LongWritable value : values)
{
sum = sum + value.get();
}
context.write(key, new LongWritable(sum));
}
}

public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
{
Configuration conf = new Configuration();


//Enable JVM Profiler, the profiler name is HPROF Profiler.......
conf.setBoolean("mapreduce.task.profile", true);
conf.set("mapreduce.task.profile.params", "-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
conf.set("mapreduce.task.profile.maps", "0-2");
conf.set("mapreduce.task.profile.reduces", "");


Job job = new Job(conf, "Finding the stock Volume");
job.setJarByClass(MapReduceProfiler.class);

job.setMapperClass(MyMapper.class);
job.setCombinerClass(MyReducer.class);
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
}
