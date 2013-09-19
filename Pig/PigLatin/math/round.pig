mydata = load '/stats/number' as (num:double);
round = foreach mydata generate ROUND(num);
dump round;
