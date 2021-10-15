// Databricks notebook source
// DBTITLE 1,Hive Table Creation
## Creating orders table

// COMMAND ----------

spark.sql("CREATE TABLE orders_raj1( order_id int, order_date timestamp, order_number int, order_status string) row format delimited fields terminated by ',' location '/FileStore/tables/Target' ")

// COMMAND ----------

spark.sql("show create table orders_raj1").show(false)

// COMMAND ----------

spark.sql("select * from orders_raj1 limit 2").show()

// COMMAND ----------

## Creating Orders table with ORC file format.

Orc and Parquet file formats gives best perfomance in Hadoop enviornment as by default this files has some intelligence.

They use something called predicate and projection pushdown. 

For eg : If we want to retrive a particular record which uses ORC/Parquet format then it directly goes and fetch particular record where as in normal file format it has to scan the entire file and then retrieve the required value

// COMMAND ----------

spark.sql("create table orders_orc (Order_id int,order_date timestamp,order_number int,order_status string) stored as orc location '/FileStore/tables/Orders_Orc'")

// COMMAND ----------

spark.sql("show create table orders_orc").show(false)

// COMMAND ----------

## Inserting data from normal table to ORC format table

// COMMAND ----------

spark.sql("insert into table orders_orc select * from orders_raj1")

// COMMAND ----------

spark.sql("select count(*) from orders_orc").show()

// COMMAND ----------

## Creating orders table with table properities.Skipping header and footer records while creating table
Table properties are not in databricks but they are working in cloudera

// COMMAND ----------

spark.sql("CREATE TABLE orders_header_skip2( Order_id int, order_date timestamp, order_number int, order_status String ) row format delimited fields terminated by ',' location 'dbfs:/FileStore/tables/Target' TBLPROPERTIES('skip.header.line.count'=100)");

// COMMAND ----------

spark.sql("select count(*) from orders_header_skip2").show() 

// COMMAND ----------

## Converting managed table to external table

// COMMAND ----------

spark.sql("ALTER TABLE orders_header_skip2 SET TBLPROPERTIES(\"EXTERNAL\"=\"TRUE\")")

// COMMAND ----------

## Creating HIVE table with Immutable property

Immutable will help not have duplicates while loading into table

// COMMAND ----------

spark.sql("CREATE TABLE orders_header_skip_immu( Order_id int, order_date timestamp, order_number int, order_status String ) row format delimited fields terminated by ',' location '/FileStore/tables/Target' TBLPROPERTIES('immutable'='true')");

// COMMAND ----------

spark.sql("select count(*) from orders_header_skip_immu" ).show()

// COMMAND ----------

spark.sql("insert into table orders_header_skip_immu select * from orders_raj1 ")

// COMMAND ----------

spark.sql("select count(*) from orders_header_skip_immu").show()
