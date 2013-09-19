import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;

public class ConversionExtraction
{
        public static class MapClass extends Mapper<LongWritable, Text, Text, Text>
        {
        	StringBuilder emitValue = null;
        	StringBuilder emitKey = null;
        	Text kword = new Text();
        	Text vword = new Text();
        	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        	{
                        String parts[];
                        String line = value.toString();
                        parts = line.split("\\,");
                        emitValue = new StringBuilder(1024);
                        emitKey = new StringBuilder(1024);
                        if(parts.length == 11)
                        {
                        		emitKey.append(parts[0]).append("\t").append(parts[1]);
                        		emitValue.append(parts[3]).append(",").append(parts[6]).append(",").append(parts[9]);
                        		kword.set(emitKey.toString());
                        		vword.set(emitValue.toString());
                        		context.write(kword, vword);
                        }
        	}
        }
        public static class ReduceClass extends Reducer<Text, Text, Text, Text>
        {
        	List<Long> clicks = null;
        	List<Long> conversions = null;
        	Text vword = new Text();
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                {
                	clicks = new ArrayList<Long>();
                	conversions = new ArrayList<Long>();
                	for(Text val : values)
                	{
                		String valParts[] = val.toString().trim().split("\\,");
                		int num = Integer.valueOf(valParts[2]);
                		if(num == 505559)
                		{
                	                try
                                        {
                			        long seconds = TimeConversion.getTime(valParts[0].trim());
                				conversions.add(seconds);
                			}
                                        catch (ParseException e)
                                        {
                				e.printStackTrace();
                			}
                		}
                		else
                		{
                			String condition = valParts[1].trim();
                			if(condition.toLowerCase().equals("true"))
                			{
                		        	try
                                                {
                					long seconds = TimeConversion.getTime(valParts[0].trim());
                					clicks.add(seconds);
                				}
                                                catch (ParseException e)
                                                {
                					e.printStackTrace();
                				}
                			}
                		}
                	}
                	long conSeconds = 0L;
                	long clkSeconds = 0L;
                	String latestConTime = "NULL";
                	if(conversions.size() > 0)
                	{
                		conSeconds = Collections.max(conversions);
                		latestConTime = TimeConversion.getDateString(conSeconds);
                		if(clicks.size() >0)
                        	{
                        		clkSeconds = Collections.max(clicks);
                        	}
                        	if(conSeconds > clkSeconds)
                        	{
                        		StringBuilder emitValue = new StringBuilder(1024);
                        		emitValue.append(latestConTime);
                        		vword.set(emitValue.toString());
                        		context.write(key, vword);
                        	}
                	}
                }
        }
        public static void main(String args[]) throws Exception
        {
                Configuration conf = new Configuration();
                String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if(otherArgs.length != 2)
                {
                        System.out.println("Usage: ConversionExtraction <input> <output>");
                        System.exit(1);
                }
               	Job job = new Job(conf, "This MapReduce job for ConversionExtraction");
                job.setJarByClass(ConversionExtraction.class);

                job.setMapperClass(MapClass.class);
                job.setReducerClass(ReduceClass.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
