package com.raj.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDataRead {

	/**
	 * @param args
	 */
	public static final Configuration conf = HBaseConfiguration.create();
	public static void main(String[] args) throws IOException {
		HTable table = new HTable(conf, "stocks");
		SingleColumnValueFilter filter = new SingleColumnValueFilter("nyse".getBytes(), "open".getBytes(), CompareOp.EQUAL, "26.16".getBytes());
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter("nyse".getBytes(), "close".getBytes(), CompareOp.GREATER, "26.16".getBytes());
		Scan scan = new Scan();
		scan.setFilter(filter);
		scan.setFilter(filter1);
		ResultScanner rs = table.getScanner(scan);
		for(Result r = rs.next(); r!=null; r=rs.next())
		{
			System.out.print(Bytes.toString(r.getRow()) + "\t");
			System.out.println(Bytes.toString(r.getValue("nyse".getBytes(), "open".getBytes())));
		}
	}
}
