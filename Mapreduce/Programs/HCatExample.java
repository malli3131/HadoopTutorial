package hadoop.training;

import java.io.IOException; 

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hcatalog.data.DefaultHCatRecord; 
import org.apache.hcatalog.data.HCatRecord; 
import org.apache.hcatalog.data.schema.HCatSchema; 
import org.apache.hcatalog.mapreduce.HCatInputFormat; 
import org.apache.hcatalog.mapreduce.HCatOutputFormat; 
import org.apache.hcatalog.mapreduce.OutputJobInfo; 

public class HCatExample { 

	/** 
	 * @param args 
	 * @author Nagamallikarjuna
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */ 
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException { 
		Configuration conf = new Configuration(); 
		Job job = new Job(conf, "Read Data from Table"); 

		HCatInputFormat.setInput(job, "default", "people"); 
		HCatOutputFormat.setOutput(job, OutputJobInfo.create("default", "names", null)); 

		job.setJarByClass(HCatExample.class); 
		job.setMapperClass(MyMapper.class); 
		job.setReducerClass(MyReducer.class); 

		job.setMapOutputKeyClass(Text.class); 
		job.setMapOutputValueClass(Text.class); 

		job.setOutputKeyClass(WritableComparable.class); 
		job.setOutputValueClass(DefaultHCatRecord.class); 

		job.setInputFormatClass(HCatInputFormat.class); 
		job.setOutputFormatClass(HCatOutputFormat.class); 

		@SuppressWarnings("deprecation") 
		HCatSchema schema = HCatOutputFormat.getTableSchema(job); 
		HCatOutputFormat.setSchema(job, schema); 

		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	} 

	@SuppressWarnings("rawtypes") 
	public static class MyMapper extends Mapper<WritableComparable, HCatRecord, Text, Text> 
	{ 
		public void map(WritableComparable key, HCatRecord value, Context context) throws IOException, InterruptedException 
		{ 
			String name = (String) value.get(1); 
			String place = (String) value.get(2); 
			context.write(new Text(name), new Text(place)); 
		} 
	} 
	@SuppressWarnings("rawtypes") 
	public static class MyReducer extends Reducer<Text, Text, WritableComparable, HCatRecord> 
	{ 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{ 
			String name = key.toString(); 
			String place = ""; 
			for(Text value : values) 
			{ 
				place = value.toString(); 
			} 
			HCatRecord record = new DefaultHCatRecord(2); 
			record.set(0, name); 
			record.set(1, place); 
			context.write(null, record); 
		} 
	} 
}
