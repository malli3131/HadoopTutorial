mydata = load '/stats/number' as (num:double);
cosh = foreach mydata generate COSH(num);
dump cosh;
