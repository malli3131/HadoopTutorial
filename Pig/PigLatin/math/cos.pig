mydata = load '/stats/number' as (num:double);
cos = foreach mydata generate COS(num);
dump cos;
