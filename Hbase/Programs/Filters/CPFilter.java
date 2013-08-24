package com.raj.hbase.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CPFilter {

	/**
	 * @param args
	 */
		public static Configuration conf = HBaseConfiguration.create();
		public static void main(String[] args) throws IOException {
			HTable table = new HTable(conf, "stocks");
			//The filter OR filter1
			Scan scan = new Scan();
			scan.addFamily("nyse".getBytes());
			ColumnPrefixFilter cpf = new ColumnPrefixFilter("volume".getBytes());
			scan.setFilter(cpf);
			ResultScanner rs = table.getScanner(scan);
			for(Result r = rs.next(); r!=null; r=rs.next())
			{
				System.out.println(Bytes.toString(r.getRow()));
			}
		}
	}