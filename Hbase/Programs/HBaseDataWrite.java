package com.twok.hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

public class HBaseDataWrite {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 */
	public static Configuration conf = HBaseConfiguration.create();
	public static void main(String[] args) throws IOException {
		HTable mytable = new HTable(conf, "stocks");
		File file = new File("/home/prasad/training/stocks");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		Put data = null;
		while(line != null)
		{
			String parts[] = line.split("\\t");
			if(parts.length == 9)
			{
				String myRow = parts[1] + ":" + parts[2];
				data = new Put(myRow.getBytes());
				data.add("nyse".getBytes(), "open".getBytes(), parts[3].getBytes());
				data.add("nyse".getBytes(), "high".getBytes(), parts[4].getBytes());
				data.add("nyse".getBytes(), "low".getBytes(), parts[5].getBytes());
				data.add("nyse".getBytes(), "close".getBytes(), parts[5].getBytes());
				data.add("nyse".getBytes(), "volume".getBytes(), parts[7].getBytes());
				data.add("nyse".getBytes(), "adj_close".getBytes(), parts[8].getBytes());
				mytable.put(data);
			}
			line = br.readLine();
		}
	}

}
