package com.raj.hbase.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MCPFilter {

	/**
	 * @param args
	 */
		public static Configuration conf = HBaseConfiguration.create();
		public static void main(String[] args) throws IOException {
			HTable table = new HTable(conf, "stocks");
			//The filter OR filter1
			byte[][] prefixes = {Bytes.toBytes("open"), Bytes.toBytes("close")};
			Scan scan = new Scan();
			scan.addFamily("nyse".getBytes());
			MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(prefixes);
			scan.setFilter(mcpf);
			ResultScanner rs = table.getScanner(scan);
			for(Result r = rs.next(); r!=null; r=rs.next())
			{
				System.out.println(Bytes.toString(r.getRow()));
			}
		}
	}