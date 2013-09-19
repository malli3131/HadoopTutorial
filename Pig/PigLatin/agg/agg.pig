mydata = load '/stats/number' as (num:double);
gprd = group mydata by num;
agg = foreach gprd generate group, AVG(mydata.num);
dump agg;
