package twok.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

public class WriteHDFS {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {	
		Configuration conf = new Configuration();
		String otherArgs[] = new GenericOptionsParser(args).getRemainingArgs();
		String localfile = otherArgs[0];
		String hdfsfile = otherArgs[1];
		FileSystem local = FileSystem.getLocal(conf);
		FileSystem hdfs = FileSystem.get(new URI("hdfs://master:9000"), conf);
		FSDataInputStream in = local.open(new Path(localfile));
		FSDataOutputStream out = hdfs.create(new Path(hdfsfile));
		byte[] buffer = new byte[256];
		while(in.read(buffer) > 0)
		{
			out.write(buffer, 0, 256);
		}
		out.close();
		in.close();
	}

}