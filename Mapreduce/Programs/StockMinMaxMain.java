package twok.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StockMinMaxMain {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(args.length != 3)
		{
			System.out.println("Usage is: hadoop jar jarname mainclass input output option");
			System.out.println("Avalable options are open | high | low | close");
			System.exit(1);
		}
		
		//To make Dynamic this program to work with multiple columns....
		
		Map<String, Integer> options =  new HashMap<String, Integer>();
		options.put("open", 3);
		options.put("high", 4);
		options.put("low", 5);
		options.put("close", 6);
		
		String option = otherArgs[2];
		conf.setInt("name", options.get(option));
		
		Job job = new Job(conf, "Find the volume of Stocks");
		job.setJarByClass(StockMinMaxMain.class);
		
		job.setMapperClass(StockMinMaxMapper.class);
		job.setReducerClass(StockMinMaxReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
		}
}