mydata = load '/stats/number' as (num:double);
asin = foreach mydata generate ASIN(num);
dump asin;
