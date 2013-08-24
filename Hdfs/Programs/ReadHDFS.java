package twok.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadHDFS {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 * @throws URISyntaxException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		Path fileName = new Path(args[0]);
		Pattern pattern = Pattern.compile("^part.*");
		Matcher matcher = null;
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		FileStatus files[] = hdfs.listStatus(fileName);
		for(FileStatus file: files)
		{
			matcher = pattern.matcher(file.getPath().getName());
			if(matcher.matches())
			{
				FSDataInputStream in = hdfs.open(file.getPath());
				in.seek(0);
				IOUtils.copyBytes(in, System.out, conf, false);
			}
		}
	}
}
