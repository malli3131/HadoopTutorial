nums = load '/pig/numbers' using PigStorage() as (num1:int, num2:int);
arth = foreach nums generate num1, num2, num1 + num2 as add, num2 - num1 as sub, num1 * num2 as mul;
dump arth;
