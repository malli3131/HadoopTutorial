mydata = load '/stats/number' as (num:double);
atan = foreach mydata generate ATAN(num);
dump atan;
