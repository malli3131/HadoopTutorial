mydata = load '/stats/number' as (num:double);
log = foreach mydata generate LOG(num);
dump log;
