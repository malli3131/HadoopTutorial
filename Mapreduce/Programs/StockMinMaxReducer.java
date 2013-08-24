package twok.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StockMinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text>
{
	Text vword = new Text();
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	{
		double min = Double.MAX_VALUE;
		double max = 0.0;
		for(DoubleWritable value : values)
		{
			double myvalue = value.get();
			max = (max > myvalue) ? max : myvalue;
			min = (min < myvalue) ? min : myvalue;
		}
		String minmax = "Min is " + min + ", Max is " + max;
		vword.set(minmax);
		context.write(key, vword);
	}
}
