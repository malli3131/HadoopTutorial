mydata = load '/stats/number' as (num:double);
ceil = foreach mydata generate CEIL(num);
dump ceil;
