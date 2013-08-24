package twok.hadoop.mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.Set;

public class TemperatureMinMax
{
        public static class MapClass extends Mapper<LongWritable, Text, Text, MapWritable>
        {
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        String parts[];
                        String line = value.toString();
                        parts = line.split("\\,");
                        Text high = new Text(parts[1]);
                        Text low = new Text(parts[2]);
                        MapWritable map = new MapWritable();
                        map.put(new Text("high"), high);
                        map.put(new Text("low"), low);
                        context.write(new Text(parts[0]), map);
                }
        }
        public static class ReduceClass extends Reducer<Text, MapWritable, Text, Text>
        {
                public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
                {
                	String con = "";
                	for(MapWritable value : values)
                    {
                        Set<Writable> keys = value.keySet();
                        for(Writable w : keys)
                        {
                        	con += w.toString() + ":";
                        	Writable val = value.get(w);
                        	con += val.toString() + ",";
                        }
                    }
                	context.write(key, new Text(con));
                }
        }
public static void main(String args[]) throws Exception
{
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if(otherArgs.length != 2)
    {
            System.out.println("Usage: MapCount <in> <out>");
            System.exit(2);
    }
    Job job = new Job(conf, "Testing MapReduce");
    job.setJarByClass(TemperatureMinMax.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
