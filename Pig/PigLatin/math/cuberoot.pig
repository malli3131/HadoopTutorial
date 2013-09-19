mydata = load '/stats/number' as (num:double);
croot = foreach mydata generate CBRT(num);
dump croot;
