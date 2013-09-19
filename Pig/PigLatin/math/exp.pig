mydata = load '/stats/number' as (num:double);
exp = foreach mydata generate EXP(num);
dump exp;
