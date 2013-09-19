package twok.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
public class SequenceFileWrite {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		String name = "/home/naga/bigdata/hadoop-1.0.3/jobs/daily";
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(name));
		String line = br.readLine();
		String uri = "/sequenceFiles/stocks";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		Path path = new Path(uri);
		Text key = new Text();
		LongWritable value = new LongWritable();
		SequenceFile.Writer writer = null;
		try 
		{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			while(line != null)
			{
				String parts[] = line.split("\\t");
				key.set(parts[1]);
				value.set(Long.valueOf(parts[7]));
				writer.append(key, value);
				line = br.readLine();
			}
			
		}
		finally {
			IOUtils.closeStream(writer);
		}
	}
}