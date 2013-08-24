import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;


public class DataLoadHbase {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "stocks");
		File file = new File("/home/naga/bigdata/hadoop-1.0.3/daily");
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = br.readLine();
		Put data = null;
		while(line != null)
		{
			String parts[] = line.trim().split("\\t");
			if(parts.length == 9)
			{
				String key = parts[1] + ":" + parts[2];
				data = new Put(key.getBytes());
				data.add("cf".getBytes(), "exchange".getBytes(), parts[0].getBytes());
				data.add("cf".getBytes(), "open".getBytes(), parts[3].getBytes());
				data.add("cf".getBytes(), "high".getBytes(), parts[4].getBytes());
				data.add("cf".getBytes(), "low".getBytes(), parts[5].getBytes());
				data.add("cf".getBytes(), "close".getBytes(), parts[6].getBytes());
				data.add("cf".getBytes(), "volume".getBytes(), parts[7].getBytes());
				data.add("cf".getBytes(), "adj_close".getBytes(), parts[8].getBytes());
				table.put(data);
			}
			line = br.readLine();
		}
		br.close();
		table.close();
	}

}
