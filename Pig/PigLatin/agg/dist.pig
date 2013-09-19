mydata = load '/stats/info' using PigStorage() as (company:chararray, sales:double);
uniq = distinct mydata;
gprd = group uniq by company;
cntd = foreach gprd generate group, COUNT(uniq.sales) as total;
ord = order cntd by total desc;
dump ord; 

stats = foreach gprd generate group, MIN(uniq.sales) as min, MAX(uniq.sales) as max, MEAN(uniq.sales);
