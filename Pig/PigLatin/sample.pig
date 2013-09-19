data = load '/daily' using PigStorage();
test = sample data 0.01;
dump test;
