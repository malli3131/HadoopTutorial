Sqoop Documentation:
--------------------
Sqoop is a Bi-directional tool to migrate data between RDBMS Systems to Hadoop/HBase/Hive vice versa.
It is having a list of commmands like 

Installation:
-------------
To install sqoop, do the following steps.

  1. Download Sqoop from the Apache Sqoop Website.
  2. Extract the tar file and place the extracted sqoop directory into your favourite directory.
  3. Modify the /home/$USER/.bashrc file to add HADOOP_HOME, SQOOP_HOME, and adding the executable of Hadoop, Sqoop to
     System Path variable called PATH, which makes commands like hadoop, sqoop are available acorss the System
  4. Download the JDBC Driver. Let us assume we are migrating data from MySQL to Hadoop, Download the MySQL JDBC driver
     from the MySQL website. This is true for other RDBMS Systems like Oracle, DB2 etc...
  5. Place the Downloaded JDBC driver into Hadoop lib directory as well as Sqoop lib directory.
  
Run the sqoop command in terminal, it will display try 'sqoop help'

cmd> sqoop help
  
usage: sqoop COMMAND [ARGS]

  Available commands:
    codegen            Generate code to interact with database records
    create-hive-table  Import a table definition into Hive
    eval               Evaluate a SQL statement and display the results
    export             Export an HDFS directory to a database table
    help               List available commands
    import             Import a table from a database to HDFS
    import-all-tables  Import tables from a database to HDFS
    job                Work with saved jobs
    list-databases     List available databases on a server
    list-tables        List available tables in a database
    merge              Merge results of incremental imports
    metastore          Run a standalone Sqoop metastore
    version            Display version information

  See 'sqoop help COMMAND' for information on a specific command
-----------------------------------------------------------------------------------------------------------
To Migrate a sample MySQL Table data from MySQL to Hadoop. Do the following things
MySQL is not running in your machine, please install it by refering Hadoop_Lab_Information.pdf document.

cmd> mysql -u username -p
password: "enter your password"

cmd> create database hadoop;
cmd> use hadoop;
cmd> create table stocks (exchange varchar(50), symbol varchar(50), stock varchar(50), volume bigint(11));

cmd> insert into stocks (exchange, symbol, stock, volume) values ('NYSE', 'GOOG', 'Google', 12000000);
cmd> insert into stocks (exchange, symbol, stock, volume) values ('BSE', 'SBI', 'State Bank of India', 100000);
cmd> insert into stocks (exchange, symbol, stock, volume) values ('BSE', 'INFY', 'Infosys Technologies Ltd', 1000000);
cmd> insert into stocks (exchange, symbol, stock, volume) values ('BSE', 'TCS', 'Tata Consultancy Services', 1500000);
-----------------------------------------------------------------------------------------------------------------

Sqoop Commands Uasage:

1. version
  
    sqoop version
    
    Sqoop 1.3.0
    git commit id 4859bdbf89fa3492db3c19c15054966e1bb1b628
    Compiled by arvind@ubuntu on Sat Jun 18 14:56:19 PDT 2011

2. codegen
  
    sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --table stocks --username root -P
    password: "eneter your db password for username"
    
    The above command will generate the class name called stocks.java in the current directory where the above command
    issued. The generated code is used to interact the with database records.
    
3. list-databases

    sqoop list-databases --connect jdbc:mysql://localhost:3306/ --username root -P
    
    The above command displays the all the databases in the MySQL server.
    
    Example Output:
    ---------------
    13/09/19 02:21:30 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
    13/09/19 02:21:31 INFO manager.SqlManager: Executing SQL statement: SHOW DATABASES
    information_schema
    hadoop
    hivemetastore
    mysql
    performance_schema
    test

4. list-tables 
    
    sqoop list-tables --connect jdbc:mysql://localhost:3306/hadoop --username root -P
    
    The above command will display all the tables under the database name called hadoop.
    
    Example Output:
    ---------------
    13/09/19 02:23:50 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
    indix
    nyse
    stocks

5. eval
    
    sqoop eval --connect jdbc:mysql://localhost:3306/hadoop --username root -P -e "select * from stocks limit 10"
    
    The above command is good for evaluating the SQL code, It is always better to check with eval before doing any bulk imports.

6. import
    
    sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root -P --table stocks -m 1 --target-dir /sqoop/stocks
    
    The above command will import the stocks table data into an HDFS directory called /sqoop/stocks
    
    sqoop-import -D mapreduce.job.reduces=2 --connect jdbc:mysql://10.17.80.90:3306/hadoop --username root --password root
    --table people --hbase-create-table --hbase-table people --column-family info --split-by sno --hbase-row-key sno -hbase-bulkload 
    -m 2 --target-dir /mydir/docbulkload
 
7. export

    sqoop export --connect jdbc:mysql://localhost:3306/hadoop --username root -P --export-dir /sqoop/stocks/ --table nyse

    The above command is used to export data from HDFS to MySQL table called nyse, Here MySQL field delimiter ','. If the
    data in HDFS file is delimited by ',' no need to any changes. If the data in HDFS is delimited by '\t' we have to add
    another flag like --fields-terminated-by '\t'.

8. import-all-tables

    sqoop import-all-tables --connect jdbc:mysql://localhost:3306/hadoop --username root -P -m 1  --warehouse-dir /hadoop/
    
    The above command will import all the tables of MySQL hadoop database into HDFS directory called /hadoop
    
9.   TO BE UPDATED........




```
 2019  sqoop
 2020  sqoop help
 2021  sqoop version
 2023  sqoop help
 2024  sqoop list-databases --help
 2025  sqoop list-databases --connect jdbc:mysql://localhost:3306/ --username root --password root
 2026  sqoop list-databases --connect jdbc:mysql://localhost:3306/ --username root -P
 2027  sqoop list-tables --help
 2028  sqoop list-tables --connect jdbc:mysql://localhost:3306/hadoop --username root -P
 2029  sqoop help
 2031  sqoop eval
 2032  sqoop eval --help
 2033  sqoop eval --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query "select pname, company from projects;"
 2034  sqoop eval --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query "select pname, company from projects limit 2;"
 2036  sqoop help
 2037  sqoop codegen --help
 2038  #sqoop codegen --connect jdbc:mysql
 2040  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects
 2044  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --class Project
 2045  sqoop codegen --help
 2046  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --class-name Project
 2052  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --package-name com.data.sqoop.Project
 2054  cd com/data/sqoop/Project/
 2058  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query "select pname from projects;"
 2060  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query 'select pname from projects;'
 2061  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query 'select pname from projects where $CONDITIONS'
 2064  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query 'select pname from projects where $CONDITIONS' --class-name Query
 2068  sqoop help
 2069  sqoop codegen --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query 'select pname from projects where $CONDITIONS AND company="facebook"' --class-name Query
 2073  sqoop help
 2074  sqoop import --help
 2075  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/
 2076  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/project_avro/ -m 2 --as-avrodatafile
 2077  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/project_par/ -m 2 --as-parquetfile
 2078  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 
 2081  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir
 2082  sqoop import --help
 2083  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2086  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2090  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query 'select pname, company from projects where $CONDITIONS' --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2091  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --query 'select pname, company from projects where $CONDITIONS' --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t' --split-by pname
 2093  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2094  sqoop import --help
 2095  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --incremental lastmodified --username root --password root --table projects --target-dir /sqoop/projects/ -m 1  --fields-terminated-by '\t' --check-column pid --last-value 9
 2096  sqoop import --connect jdbc:mysql://localhost:3306/hadoop --incremental lastmodified --username root --password root --table projects --target-dir /sqoop/projects/ -m 1  --fields-terminated-by '\t' --check-column pid --last-value 9 --append
 2097* sqoop import --connect jdbc:mysql://localhost:3306/hadoop --incremental append --username root --password root --table projects --target-dir /sqoop/projects/ -m 1  --fields-terminated-by '\t' --check-column pid  --append
 2100  sqoop help
 2102  sqoop import-all-tables --help
 2103  sqoop import-all-tables --connect jdbc:mysql://localhost:3306/hadoop --warehouse-dir /sqoop/ --username root --password root -m 2
 2108  sqoop export --help
 2109  sqoop export --connect jdbc:mysql://localhost:3306/hadoop --export-dir /pig/volume --table volume --username root --password root -m 2
 2111  sqoop export --help
 2112  sqoop export --connect jdbc:mysql://localhost:3306/hadoop --export-dir /pig/volume --table volume --username root --password root -m 2 --input-fields-terminated-by '|'
 2114  sqoop help
 2115  sqoop merge --help
 2117  sqoop help
 2118  sqoop metastore
 2120  sqoop job --help
 2121  sqoop job -list
 2122  sqoop job -list --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 2123  sqoop job --create import_projects_table -- import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2125  sqoop job -list
 2126  sqoop job -list --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 2127  sqoop job --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop --create import_projects_table -- import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2128  sqoop job -list --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 2129  sqoop job --delete import_projects_table
 2130  sqoop job -list
 2131  sqoop job -list --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 2132  sqoop job -exec import_projects_table
 2133  sqoop job --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop --delete import_projects_table
 2134  sqoop job --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop --create import_projects_table -- import --connect jdbc:mysql://localhost:3306/hadoop --username root --password root --table projects --target-dir /sqoop/projects/ -m 2 --delete-target-dir --fields-terminated-by '\t'
 2135  sqoop job --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop --exec import_projects_table
 2136  sqoop job -list --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 2137  sqoop job -show --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 2138  sqoop job --help
 2139  sqoop job --show import_projects_table --meta-connect  jdbc:hsqldb:hsql://localhost:16000/sqoop
 ```
      
        
