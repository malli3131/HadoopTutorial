--This pig script is used to aggregated the top 10 volumes of each and every stock
--Load the data from HDFS and Delimiter is ",".
stocks = load '/nyse/stocks' using PigStorage(',') as (ex:chararray, stock:chararray, sdate:chararray, open:double, high:double, low:double, close:double, volume:long, adj_close:double);
--Project the stock and volume columns...
reqitems = foreach stocks generate stock, volume;
--group reqitems by stock
grpstocks = group reqitems by volume;
--Sort the stocks data and limit top10 for every stock...
top10 = foreach grpstocks { sortorder = order reqitems by volume desc; top = limit sortorder 10; generate group, flatten(top);}
mygroup = group top10 by top::stock;
mysum = foreach mygroup generate group, SUM(top10.top::volume);
dump mysum;
