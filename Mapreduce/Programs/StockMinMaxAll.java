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
import org.apache.hadoop.util.GenericOptionsParser;

public class StockMinMaxAll {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	
		//Configuring/setting up MapReduce Driver
		Configuration conf = new Configuration();
		String otherArgs[] = new  GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "Fidning the mix and max of open, high, close, low, adj_close, volume by using StockWritable Custom Data Type");
		
		job.setJarByClass(StockMinMaxAll.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StockWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	//Mapper Class for implementing Map side Business Logic
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StockWritable>
	{
		Text emitKey = new Text();
		StockWritable emitValue = new StockWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String parts[] = line.split("\\t");
			if(parts.length == 9)
			{
				emitKey.set(parts[1]);
				emitValue.setOpen(Double.valueOf(parts[3]));
				emitValue.setHigh(Double.valueOf(parts[4]));
				emitValue.setLow(Double.valueOf(parts[5]));
				emitValue.setClose(Double.valueOf(parts[6]));
				emitValue.setVolume(Long.valueOf(parts[7]));
				context.write(emitKey, emitValue);
			}
		}
	}
	
	//Reducer Class for implementing Reducer side Business Logic
	public static class MyReducer extends Reducer<Text, StockWritable, Text, Text>
	{
		Text emitValue  = new Text();
		public void reduce(Text key, Iterable<StockWritable> values, Context context) throws IOException, InterruptedException
		{
			double current = 0.0;
			double minOpen = Double.MAX_VALUE;
			double maxOpen = 0.0;
			double minHigh = Double.MAX_VALUE;
			double maxHigh = 0.0;
			double minLow = Double.MAX_VALUE;
			double maxLow = 0.0;
			double minClose = Double.MAX_VALUE;
			double maxClose = 0.0;
			long myCurrent = 0L;
			long minVolume = Long.MAX_VALUE;
			long maxVolume = 0L;
			int counter = 0;
			for(StockWritable value: values)
			{
				current = value.getOpen();
				minOpen = (minOpen < current) ? minOpen : current;
				maxOpen = (maxOpen > current) ? maxOpen : current;
				current = value.getHigh();
				minHigh = (minHigh < current) ? minHigh : current;
				maxHigh = (maxHigh > current) ? maxHigh : current;
				current = value.getLow();
				minLow = (minLow < current) ? minLow : current;
				maxLow = (maxLow > current) ? maxLow : current;
				current = value.getClose();
				minClose = (minClose < current) ? minClose : current;
				maxClose = (maxClose > current) ? maxClose : current;
				myCurrent = value.getVolume();
				minVolume = (minVolume < myCurrent) ? minVolume : myCurrent;
				maxVolume = (maxVolume > myCurrent) ? maxVolume : myCurrent;
				counter++;
			}
			String content = "Open: " + minOpen + " - " + maxOpen + "\t" + "High: " + minHigh + " - " + maxHigh + "\t" + "Low: " + minLow + "-" + maxLow + "\t" + "Close: " + minClose + " - " + maxClose + "\t" + "Volume: " + minVolume + " - " + maxVolume + "\t" + "Total Records" + counter;
			emitValue.set(content);
			context.write(key, emitValue);
		}
	} 
}
