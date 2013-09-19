mydata = load '/stats/number' as (num:double);
floor = foreach mydata generate FLOOR(num);
dump floor;
