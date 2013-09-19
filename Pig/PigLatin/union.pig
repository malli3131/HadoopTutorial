names = load '/pig/names' using PigStorage() as (fname:chararray, lname:chararray);

ages = load '/pig/ages' using PigStorage() as (age:int);

record = union names, ages;

dump record;
