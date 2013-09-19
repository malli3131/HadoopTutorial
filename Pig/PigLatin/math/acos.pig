mydata = load 'number' as (num:double);
acos = foreach mydata generate ACOS(num);
dump acos;
