package hadoop.training;

import java.io.IOException; 
import java.text.DateFormatSymbols; 
import java.util.Calendar; 
import java.util.GregorianCalendar; 
import java.util.Iterator; 

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
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat; 

public class StockMonthVolume { 

	/** 
	 * @param args 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */ 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException { 
		Configuration conf = new Configuration(); 
		JobConf job = new JobConf(conf); 

		job.setJobName("Finding Avg and Sum of Stock Volume"); 

		job.setJarByClass(StockMonthVolume.class); 
		//Mapper and Reducer classes 
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class); 

		//Output Key-Value Data types 
		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(LongWritable.class); 

		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class); 

		//Inform Input/Output Formats 
		job.setInputFormat(TextInputFormat.class); 
		job.setOutputFormat(MyMultipleOutputFileFormat.class); 

		//Inform file or Directory locations 
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 

		//Inform termination criteria 
		JobClient.runJob(job); 

	} 

	//This is my Mapper Class 
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> 
	{ 
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, LongWritable> collect, Reporter reporter) 
		throws IOException { 
			String line = value.toString(); 
			String parts[] = line.trim().split("\\t"); 
			if(parts.length == 9) 
			{ 
				String month = parts[2]; 
				String[] monParts = month.split("\\-"); 
				if(monParts.length == 3) 
				{ 
					Calendar cal = new GregorianCalendar(Integer.valueOf(monParts[0]), Integer.valueOf(monParts[1])-1, Integer.valueOf(monParts[2])); 
					int myMonth = cal.get(Calendar.MONTH); 
					String strMonth = new DateFormatSymbols().getMonths()[myMonth]; 
					long volume = Long.valueOf(parts[7]); 
					collect.collect(new Text(strMonth), new LongWritable(volume)); 
				} 
			}
		}
	} 

	//THis is my Reducer class 
	public static class MyReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, Text> 
	{ 
		public void reduce(Text key, Iterator<LongWritable> values, 
				OutputCollector<Text, Text> collect, Reporter reporter) 
		throws IOException { 
			long sum = 0; 
			int counter = 0; 
			while(values.hasNext()) 
			{ 
				sum = sum + values.next().get(); 
				counter++; 
			} 
			float avg = (float) sum/counter; 
			String emitValue = sum + "\t" + avg; 
			collect.collect(key, new Text(emitValue));	
		} 
	} 
	public static class MyMultipleOutputFileFormat extends MultipleTextOutputFormat<Text, Text> 
	{ 
		public String generateFileNameForKeyValue(Text key, Text value, String name) 
		{ 
			return new Path(key.toString(), value.toString()).toString(); 
		} 
	} 
}