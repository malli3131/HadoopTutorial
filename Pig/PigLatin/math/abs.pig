mydata = load 'number' as (num:double);
ab = foreach mydata generate ABS(num);
dump ab;
