package com.pearson;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ComputePearsonCorrelation {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 * @Usage: hadoop jar jarname mainclassname inputlocation outputlocation
	 * @Example:- This is used to compute the Pearson Correlation coefficient between two variables....
	 * 
	 * Input matrix:
	 * 
	 * 	|c1	c2	c3
	 * 	------------------
	 * 	|2	2	-2
	 * 	|4	4	-4
	 * 	|6	6	-6
	 * 
	 * Correlation Matrix is:
	 * 
	 * 	|c1	c2	c3
	 * 	--------------------
	 *     c1|1.0	1.0	-1.0
	 *     c2|1.0	1.0	-1.0
	 *     c3|-1.0	-1.0	1.0
	 * 
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		Job job = new Job(conf, "Computing Pearson Correaltion");

		job.setJarByClass(ComputePearsonCorrelation.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(VariablePairWritable.class);
		job.setMapOutputValueClass(VariableValueWritable.class);
		job.setOutputKeyClass(VariablePairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper
			extends
			Mapper<LongWritable, Text, VariablePairWritable, VariableValueWritable> {
		VariablePairWritable emitKey = new VariablePairWritable();
		VariableValueWritable emitValue = new VariableValueWritable();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			double[] myPairs = toDouble(line.split("\\t"));

			for (int i = 0; i < myPairs.length; i++) {
				for (int j = i; j < myPairs.length; j++) {
					emitKey.setI(i);
					emitKey.setJ(j);
					emitValue.setI(myPairs[i]);
					emitValue.setJ(myPairs[j]);
					context.write(emitKey, emitValue);
				}
			}
		}

		public double[] toDouble(String[] tokens) {
			double[] myArray = new double[tokens.length];
			for (int i = 0; i < tokens.length; i++) {
				if (i == 24) {
					myArray[i] = Double.parseDouble("0.1");
				} else {
					myArray[i] = Double.parseDouble(tokens[i]);
				}
			}
			return myArray;
		}
	}

	public static class MyReducer
			extends
			Reducer<VariablePairWritable, VariableValueWritable, VariablePairWritable, DoubleWritable> {
		DoubleWritable emitValue = new DoubleWritable();

		public void reduce(VariablePairWritable key,
				Iterable<VariableValueWritable> values, Context context)
				throws IOException, InterruptedException {
			double x = 0.0d;
			double y = 0.0d;
			double xx = 0.0d;
			double yy = 0.0d;
			double xy = 0.0d;
			double n = 0.0d;
			for (VariableValueWritable value : values) {
				x += value.getI();
				y += value.getJ();
				xx += Math.pow(value.getI(), 2.0d);
				yy += Math.pow(value.getJ(), 2.0d);
				xy += value.getI() * value.getJ();
				n += 1.0d;
			}
			PearsonWritable pearson = new PearsonWritable(x, y, xx, yy, xy, n);
			double corr = pearson.getCorrelation();
			emitValue.set(corr);
			context.write(key, emitValue);
		}
	}
}
