stocks = load '/pig/stocks' as (market:chararray, stock:chararray, sdate:chararray, open:double, high:double, low:double, close:double, volume:long, adj_close:double);
aggdata = mapreduce '/home/naga/bigdata/volume.jar' store stocks into '/pig/mystocks' load '/pig/aggvolume' as (stock:chararray, aggvolume:long) `StockTotalVolume /pig/mystocks /pig/aggvolume`;
dump aggdata
