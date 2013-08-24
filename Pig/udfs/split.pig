ages = load '/pig/ages' using PigStorage() as (age:int);
split ages into young if age <25, middle if (age >25 AND age <30), old if age >30;
dump young;
dump middle;
dump old;
