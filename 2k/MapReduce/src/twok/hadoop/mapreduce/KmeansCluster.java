package twok.hadoop.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class KmeansCluster {
	public static final String hdfsPath = "/kmeans/cluster";
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		Integer[][] centroids = new Integer[2][2];
		public void setup(Context context) throws IOException
		{
			 FSDataInputStream in = null;
             BufferedReader br = null;
             FileSystem fs = FileSystem.get(context.getConfiguration());
             Path path = new Path(hdfsPath);
             in = fs.open(path);
             br  = new BufferedReader(new InputStreamReader(in));
             String line = "";
             int i=0;
             while ( (line = br.readLine() )!= null) {
             String[] arr = line.split("\\,");
             if (arr.length == 2)
             {
            	 int j=0;
            	 centroids[i][j] = Integer.valueOf(arr[0]);
                 j++;
                 centroids[i][j] = Integer.valueOf(arr[1]);
             }
             i++;
             }
             in.close();
		}
		public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException
		{
			String line = value.toString();
			String dataPoints[] = line.split("\\,");
			float distance = 0.0f;
			Text emitValue = new Text();
			Text emitKey = new Text();
			float min = Float.MAX_VALUE;
			float current = 0.0f;
			String clusterPoint = "";
			for(int i=0; i<centroids.length; i++)
			{
				int xdiff = centroids[i][0] - Integer.valueOf(dataPoints[0]);
				int ydiff = centroids[i][1] - Integer.valueOf(dataPoints[1]);
				int xcord = xdiff * xdiff;
				int ycord = ydiff * ydiff;
				distance = (float) Math.sqrt(xcord + ycord);
				current = distance;
				if(min >= current)
				{
					min = current;
					clusterPoint = centroids[i][0] + "," + centroids[i][1];
				}
			}
			String myPoint = dataPoints[0] + "," + dataPoints[1];
			emitKey.set(clusterPoint);
			emitValue.set(myPoint);
			context.write(emitKey, emitValue);
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		Text emitKey = new Text();
		Text emitValue = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			float newX = 0.0f;
			float newY = 0.0f;
			int sumX = 0;
			int sumY = 0;
			int counter = 0;
			String clusterPoints = "";
			int i = 0;
			for(Text value : values)
			{
				String line = value.toString();
				String coordinates[] = line.split("\\,");
				sumX = sumX + Integer.valueOf(coordinates[0]);
				sumY = sumY + Integer.valueOf(coordinates[1]);		
				counter++;
				if(i == 0)
				{
					clusterPoints = line;
					i = 1;
				}
				else
				{
					clusterPoints = clusterPoints + ";" + line;
				}
			}
			newX = (float) sumX/counter;
			newY = (float) sumY/counter;
			String clusterKey = "Cluster: " + key.toString();
			String clusterInfo = newX + "," + newY + "\t" + clusterPoints;
			emitKey.set(clusterKey);
			emitValue.set(clusterInfo);
			context.write(emitKey, emitValue);
		}
	} 
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.out.println("Usage is: hadoop jar jarfile MainClass input output");
			System.exit(1);
		}
		Job job = new Job(conf, "Sample Kmeans Algorithm....");
		job.setJarByClass(KmeansCluster.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
