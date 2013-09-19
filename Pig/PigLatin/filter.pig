data = load '/pig/cross' using PigStorage() as (fname:chararray,lname:chararray,age:int);
age24 = filter data by age == 24;                                                        
dump age24
