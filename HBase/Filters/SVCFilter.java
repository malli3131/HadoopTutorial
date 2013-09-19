package com.twok.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class SVCFilter {

	/**
	 * @param args
	 * @author Nagamallikarjuna
	 */
		public static Configuration conf = HBaseConfiguration.create();
		public static void main(String[] args) throws IOException {
			HTable table = new HTable(conf, "stocks");
			//The filter OR filter1
			SingleColumnValueFilter filter = new SingleColumnValueFilter("nyse".getBytes(), "open".getBytes(), CompareOp.EQUAL, "26.16".getBytes());
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter("nyse".getBytes(), "close".getBytes(), CompareOp.GREATER, "26.16".getBytes());
			Scan scan = new Scan();
			FilterList filList = new FilterList();
			filList.addFilter(filter);
			filList.addFilter(filter1);
			scan.setFilter(filList);
			ResultScanner rs = table.getScanner(scan);
			for(Result r = rs.next(); r!=null; r=rs.next())
			{
				System.out.print(Bytes.toString(r.getRow()) + "\t");
				System.out.println(Bytes.toString(r.getValue("nyse".getBytes(), "open".getBytes())));
			}
		}
	}