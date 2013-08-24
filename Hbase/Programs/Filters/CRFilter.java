package com.twok.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CRFilter {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 */
		public static Configuration conf = HBaseConfiguration.create();
		public static void main(String[] args) throws IOException {
			HTable table = new HTable(conf, "stocks");
			//The filter OR filter1
			Scan scan = new Scan();
			scan.addFamily("nyse".getBytes());
			ColumnRangeFilter crf = new ColumnRangeFilter("10.00".getBytes(), true, "30.00".getBytes(), true);
			scan.setFilter(crf);
			ResultScanner rs = table.getScanner(scan);
			for(Result r = rs.next(); r!=null; r=rs.next())
			{
				System.out.println(Bytes.toString(r.getRow()));
			}
		}
	}