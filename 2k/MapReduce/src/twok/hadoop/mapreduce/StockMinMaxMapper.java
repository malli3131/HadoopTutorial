package twok.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StockMinMaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{
	int processColumn = 0;
	public void setup(Context context)
	{
		Configuration conf = context.getConfiguration();
		processColumn = Integer.valueOf(conf.get("name"));
	}
	
	Text kword = new Text();
	DoubleWritable vword = new DoubleWritable();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		String dataParts[] = line.split("\\t");
		if(dataParts.length == 9)
		{
			kword.set(dataParts[1]);
			vword.set(Double.valueOf(dataParts[processColumn]));
			context.write(kword, vword);
		}
	}
}
