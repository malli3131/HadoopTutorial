--load the data from hbase table called stocks
data = load 'hbase://stocks' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:open cf:close', '-loadKey true limit 100') as (stockname:bytearray, open:float, close:float);
--find the change between close and open
change = foreach data generate stockname, open, close, (close - open) as change;
--store the change data into hbase table called nyse
store change into 'hbase://nyse' using org.apache.pig.backend.hadoop.hbase.HBaseStorage('cf:open cf:close cf:change', '-loadKey true');
