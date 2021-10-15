// Databricks notebook source
val df = spark.read.format("csv").load("/FileStore/tables/Orders.txt")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

val df =spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/Orders.txt")

// COMMAND ----------

df.show()

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.createOrReplaceTempView("Order")

// COMMAND ----------

val df1 =spark.sql("select * from order")

// COMMAND ----------

val df1 =spark.sql("select * from order where order_id=5")

// COMMAND ----------

df1.write.format("csv").option("header","true").mode("overwrite").save("/FileStore/tables/Testtn")

// COMMAND ----------

val test= spark.read.format("csv").option("header","true").load("/FileStore/tables/Testtn")


// COMMAND ----------

test.show()

// COMMAND ----------

val test= spark.read.format("csv").option("header","true").load("/FileStore/tables/Testtn")

// COMMAND ----------

test.show(3)

// COMMAND ----------

val df2 =spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/Order_items-2.txt")

// COMMAND ----------

df2.createOrReplaceTempView("Orderitem")

// COMMAND ----------

val df12= spark.sql("select * from orderitem")

// COMMAND ----------

val df12= spark.sql("select * from orderitem where order_item_id in(3,4)")

// COMMAND ----------

df12.write.format("csv").option("header","true").mode("overwrite").save("/FileStore/tables/Testn")

// COMMAND ----------

df12.repartition(3).write.format("csv").option("header","true").mode("overwrite").save("/FileStore/tables/Testn")

// COMMAND ----------

ddf.show()

// COMMAND ----------

val test2= spark.read.format("csv").option("header","true").load("/FileStore/tables/Testn")

// COMMAND ----------

test2.show()

// COMMAND ----------

test.join(test2,test("order_id")=== test2("order_item_id"),"outer").show(false)

// COMMAND ----------

test.join(test2,test("order_id")=== test2("order_item_id"),"left").show(false)

// COMMAND ----------

val tf1= spark.sql("select a.order_id,a.order_status,b.order_item_id,b.order_item_product_id,b.order_item_product_price from order a join orderitem b on a.order_id==b.order_item_id")

// COMMAND ----------

tf1.show()

// COMMAND ----------

val tf= spark.sql("select order_id,order_status from order o right join orderitem oi on o.order_id ==oi.order_item_id")

// COMMAND ----------

tf.show()

// COMMAND ----------

tf.show()

// COMMAND ----------

spark.sql("select * from order limit 2").show()

// COMMAND ----------

spark.sql("select * from orderitem limit 2").show()

// COMMAND ----------

spark.sql("select * from order a join orderitem b on a.order_id= b.order_item_order_id").show(2)

// COMMAND ----------

spark.sql("select substr(a.order_date,1,10) as Order_Date,SUM(b.order_item_subtotal) as order_item_subtotal  from order a Join orderitem b on a.order_id= b.order_item_order_id group by substr(a.order_date,1,10) order by 1").show()

// COMMAND ----------

spark.sql("select substr(a.order_date,1,10) as Order_Date,a.order_status as Order_Status,SUM(b.order_item_subtotal) as order_item_subtotal  from order a Join orderitem b on a.order_id= b.order_item_order_id group by 1,2 order by 1").show()

// COMMAND ----------

#Total Revenue per Order_status
# Total Quantity per status
# Total Revenue,Total Quantity , total order_item_subtotal per order_status

// COMMAND ----------

spark.sql("select * from order").show(2)

// COMMAND ----------

spark.sql("select * from orderitem").show(2)

// COMMAND ----------

spark.sql("select order_status,sum(order_item_subtotal) as total_revenue from order a join orderitem b where a.order_id=b.order_item_order_id group by order_status").show(false)

// COMMAND ----------

spark.sql("select order_status,sum(order_item_quantity) as total_qty from order a join orderitem b where a.order_id=b.order_item_order_id group by order_status").show()

// COMMAND ----------

spark.sql("select order_status,sum(order_item_quantity) as total_qty,sum(order_item_subtotal) as total_revenue from order a join orderitem b where a.order_id=b.order_item_order_id group by order_status").show()

// COMMAND ----------

spark.sql("select order_status,order_item_product_price,sum(order_item_quantity) as total_qty,sum(order_item_subtotal) as total_revenue from order a join orderitem b where a.order_id=b.order_item_order_id group by 1,2").show()

// COMMAND ----------

spark.sql("select split(order_status,'_')[1] from order").show(5)

// COMMAND ----------

val ts = sc.parallelize((1 to 100).toList,5) 

// COMMAND ----------

spark.sql("select split(order_status,'_')[0] from order").show(5)

// COMMAND ----------

ts.partitions.size 

// COMMAND ----------

ts.glom().collect 

// COMMAND ----------


