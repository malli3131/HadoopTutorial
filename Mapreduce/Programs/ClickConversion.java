import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class ClickConversion
{
	public static String hdfspath = "/user/adbigdata/mapred/conversion/output/part-r-00000";
        public static class MapClass extends Mapper<LongWritable, Text, Text, Text>
        {
        	Map<String, Long> conversionMap = null;
        	public void setup(Context context) throws IOException
        	{
        		try
        		{
				loadMap(context);
			}
			catch (ParseException e)
			{
				e.printStackTrace();
			}
        		}
        	void loadMap(Context context) throws IOException, ParseException
                {
                        FSDataInputStream in = null;
                        BufferedReader br = null;
                        FileSystem fs = FileSystem.get(context.getConfiguration());
                        Path path = new Path(hdfspath);
                        in = fs.open(path);
                        br  = new BufferedReader(new InputStreamReader(in));
                        conversionMap = new HashMap<String, Long>();
                        String line = "";
                        while ( (line = br.readLine() )!= null)
                        {
                        	String[] arr = line.trim().split("\\t");
                        	if (arr.length == 3)
                        	{
                        		String coockie = arr[0] + "\t" + arr[1];
                        		long time = TimeConversion.getTime(arr[2]);
                        		conversionMap.put(coockie, time);
                        	}
                        }
                        in.close();
               	}

        	StringBuilder emitValue = null;
        	StringBuilder emitKey = null;
        	Text kword = new Text();
        	Text vword = new Text();
        	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        	{
                        String parts[];
                        String line = value.toString();
                        parts = line.trim().split("\\t");
                        emitValue = new StringBuilder(1024);
                        emitKey = new StringBuilder(1024);
                        if(parts.length == 8)
                        {
                        	String myCoockie = parts[0] + "\t" + parts[1];
                        	long myTime = 0L;
                        	try
                        	{
					myTime = TimeConversion.getTime(parts[6]);
				}
				catch (ParseException e)
				{
					e.printStackTrace();
				}
				if(conversionMap.containsKey(myCoockie) && conversionMap.get(myCoockie) > myTime)
				{
					emitKey.append(parts[0]).append("\t").append(parts[1]).append("\t").append(parts[2]).append("\t").append(parts[3]).append("\t").append(parts[4]);
					emitValue.append(parts[5]).append(",").append(parts[6]).append(",").append(parts[7]).append(",").append(conversionMap.get(myCoockie));
					kword.set(emitKey.toString());
					vword.set(emitValue.toString());
					context.write(kword, vword);
				}
                        }
        	}
        }
        
        public static class ReduceClass extends Reducer<Text, Text, Text, Text>
        {
        	List<Long> conTimes = null;
        	long clkTime = 0L;
        	Text vword = new Text();
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                	String vparts[] = null;
                	conTimes = new ArrayList<Long>();
                	for(Text val : values)
                	{
                		vparts = val.toString().trim().split("\\,");
                		long time = 0L;
                		if(!vparts[3].equals(""))
                		{
                			time = Long.valueOf(vparts[3]);
                			conTimes.add(time);
                		}
                	}
                	if(conTimes.size() > 0)
                	{
                		long minTime = Collections.min(conTimes);
                		try
                		{
                			clkTime = TimeConversion.getTime(vparts[1]);
                		}
                		catch (ParseException e)
                		{
                			e.printStackTrace();
                		}
                		String strTime = TimeConversion.getDateString(minTime);
                		long clkCon = (minTime - clkTime) / 1000 ;
                		StringBuilder emitValue = new StringBuilder(1024);
                		emitValue.append(vparts[0]).append("\t").append(vparts[1]).append("\t").append(vparts[2]).append("\t").append(strTime).append("\t").append(clkCon);
                		vword.set(emitValue.toString());
                		context.write(key, vword);
                	}
                }
        }
        public static void main(String args[]) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if(otherArgs.length != 2)
                {
                        System.out.println("Usage: ClickConversion <input> <output>");
                        System.exit(1);
                }
               	Job job = new Job(conf, "MapReduce Job for joining click and Conversion");
                job.setJarByClass(ClickConversion.class);

                job.setMapperClass(MapClass.class);
                job.setReducerClass(ReduceClass.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
