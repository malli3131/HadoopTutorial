package com.hdfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileWrite {
	/**
	 * @param args
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException,
			URISyntaxException {
		String name = "/home/naga/dept";
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader(name));
		String line = br.readLine();
		String uri = "/nyse/";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop:9000"), conf);
		Path path = new Path(uri);
		Text key = new Text();
		Text value = new Text();
		MapFile.Writer writer = null;
		try {
			writer = new MapFile.Writer(conf, fs, uri,  key.getClass(), value.getClass());
			while (line != null) {
				String parts[] = line.split("\\t");
				key.set(parts[0]);
				value.set(parts[1]);
				writer.append(key, value);
				line = br.readLine();
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}
