ages = load '/pig/ages' using PigStorage() as (age:int);                           
data = order ages by age desc;
ltd = limit data 3;
dump data;
