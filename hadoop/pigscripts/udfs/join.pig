ranks = load '/pig/ranks' using PigStorage() as (player:chararray,con:chararray,rank:int);
country = load '/pig/country' using PigStorage() as (country:chararray, con:chararray);
--inner join
rankings = join ranks by con, country by con;
--right outer join
rankings = join ranks by con right, country by con;
--full outer join
rankings = join ranks by con full, country by con; 
--left outer join
rankings = join ranks by con left, country by con
dump rankings
