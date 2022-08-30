import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, countDistinct, dense_rank, expr, lit, max, month, rank, sum, to_date, year}
import org.apache.spark.sql.types._



object driver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("RetailStoreDataAnalysisSparkApp")
      .getOrCreate()
    System.out.println("Starting Application : RetailStoreDataAnalysisSparkApp")
    //Ref - https://anindya-saha.github.io/blog/data-science-with-spark/retail-database-analysis-python/retail-database-analysis-python.html#4.24-Get-All-Customers-who-placed-more-than-5-orders-in-August-2013:

    val custschema = StructType(List(StructField("customer_id",       IntegerType, nullable =true),
    StructField("customer_fname",    StringType, nullable =true),
      StructField("customer_lname",    StringType, nullable =true),
      StructField("customer_email",    StringType, nullable =true),
      StructField("customer_password", StringType, nullable =true),
      StructField("customer_street",   StringType, nullable =true),
      StructField("customer_city",     StringType, nullable =true),
      StructField("customer_state",    StringType, nullable =true),
      StructField("customer_zipcode",  StringType, nullable =true)
    ))
    val departments_schema = StructType(List(
      StructField("department_id",   IntegerType, nullable =true),
      StructField("department_name", StringType, nullable =true)))
    val categories_schema = StructType(List(
      StructField("category_id",            IntegerType, nullable =true),
      StructField("category_department_id", IntegerType, nullable =true),
      StructField("category_name",          StringType, nullable =true)))
    val products_schema = StructType(List(
      StructField("product_id",          IntegerType, nullable =true),
      StructField("product_category_id", IntegerType, nullable =true),
      StructField("product_name",        StringType, nullable =true),
      StructField("product_description", StringType, nullable =true),
      StructField("product_price",       FloatType, nullable =true),
      StructField("product_image",       StringType, nullable =true)))
    val orders_schema = StructType(List(
      StructField("order_id",          IntegerType, nullable =true),
      StructField("order_date",        StringType, nullable =true),
      StructField("order_customer_id", IntegerType, nullable =true),
      StructField("order_status",      StringType, nullable =true)))
    val order_items_schema = StructType(List(
      StructField("order_item_id",            IntegerType, nullable =true),
      StructField("order_item_order_id",      IntegerType, nullable =true),
      StructField("order_item_product_id",    IntegerType, nullable =true),
      StructField("order_item_quantity",      IntegerType, nullable =true),
      StructField("order_item_subtotal",      FloatType, nullable = true),
      StructField("order_item_product_price", FloatType, nullable =true)))

    val customers_df =  spark.read.option("header","true")
      .schema(custschema).csv("src/main/resources/customers.csv")
    customers_df.cache()
    customers_df.createOrReplaceTempView("customers")

    val departments_df = spark.read.option("header","true")
      .schema(departments_schema).csv("src/main/resources/departments.csv")
    departments_df.cache()
    departments_df.createOrReplaceTempView("departments")

    val categories_df = spark.read.option("header","true")
      .schema(categories_schema).csv("src/main/resources/categories.csv")
    categories_df.cache()
    categories_df.createOrReplaceTempView("categories")

    val products_df = spark.read.option("header","true")
      .schema(products_schema).csv("src/main/resources/products.csv")
    products_df.cache()
    products_df.createOrReplaceTempView("products")

    val orders_df = spark.read.option("header","true")
      .schema(orders_schema).csv("src/main/resources/orders.csv")
    orders_df.cache()
    orders_df.createOrReplaceTempView("orders")

    val order_items_df = spark.read.option("header","true")
      .schema(order_items_schema).csv("src/main/resources/order_items.csv")
    order_items_df.cache()
    order_items_df.createOrReplaceTempView("order_items")
    System.out.println("get total no of orders")
    spark.sql("select count(*) from orders").show(false)
    System.out.println(orders_df.count)

    System.out.println("get average revenue per order")
    spark.sql("select sum(order_item_subtotal)/count(distinct order_item_order_id) as avgRevenuePerorder" +
      " from orders o, order_items oi " +
      "where oi.order_item_order_id=o.order_id  ").show(false)


    orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .select(order_items_df("order_item_subtotal"),order_items_df("order_item_order_id"))
      .select(sum(order_items_df("order_item_subtotal"))/countDistinct(order_items_df("order_item_order_id")).alias("AverageRevPerOrder"))
      .show(false)

    System.out.println("get Average Revenue Per Day")
    spark.sql("select o.order_date,sum(order_item_subtotal)/count(distinct order_item_order_id) as avgRevenuePerDay" +
      " from orders o, order_items oi " +
      "where oi.order_item_order_id=o.order_id " +
      "group by o.order_date " +
      "order by o.order_date").show(false)

    orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .select(orders_df("order_date"),order_items_df("order_item_subtotal"),order_items_df("order_item_order_id"))
      .groupBy(orders_df("order_date"))
      .agg(sum(order_items_df("order_item_subtotal"))/countDistinct(order_items_df("order_item_order_id")).alias("AverageRevPerDay"))
      .sort(orders_df("order_date"))
      .show(false)
    System.out.println("get Average Revenue Per Month")
    spark.sql("select month(o.order_date) as mnth ,sum(order_item_subtotal)/count(distinct order_item_order_id) as avgRevenuePerMonth" +
      " from orders o, order_items oi " +
      "where oi.order_item_order_id=o.order_id " +
      "group by mnth " +
      "order by mnth ").show(false)

    orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .select(month(orders_df("order_date")).alias("mnth"),order_items_df("order_item_subtotal"),order_items_df("order_item_order_id"))
      .groupBy("mnth")
      .agg(sum(order_items_df("order_item_subtotal"))/countDistinct(order_items_df("order_item_order_id")).alias("AverageRevPerMonth"))
      .sort("mnth")
      .show(false)

    System.out.println("Get Total Revenue Per Month Per Year")
    spark.sql("select year(o.order_date) as yr, month(o.order_date) as mnth ," +
      "sum(order_item_subtotal) as totrev" +
      " from orders o, order_items oi " +
      "where oi.order_item_order_id=o.order_id " +
      "group by yr,mnth " +
      "order by yr,mnth ").show(false)

    orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .select(year(orders_df("order_date")).alias("yr"),
        month(orders_df("order_date")).alias("mnth"),
        order_items_df("order_item_subtotal"),
        order_items_df("order_item_order_id"))
      .groupBy("yr","mnth")
      .agg(sum(order_items_df("order_item_subtotal")).alias("TotalRev"))
      .sort("yr","mnth")
      .show(false)

    System.out.println("Top Performing Departments per year")
    spark.sql("select d.department_name as depname,year(o.order_date) as yr," +
      "sum(order_item_subtotal) as totrev" +
      " from orders o, order_items oi, products p, categories c, departments d " +
      "where oi.order_item_order_id=o.order_id " +
      "and oi.order_item_product_id=p.product_id " +
      "and p.product_category_id=c.category_id " +
      "and c.category_department_id=d.department_id " +
      "group by depname,yr " +
      "order by depname,yr ").show(false)
    val tempdf1=orders_df.join(order_items_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .join(products_df,order_items_df("order_item_product_id")===products_df("product_id"))
      .join(categories_df,products_df("product_category_id")===categories_df("category_id"))
      .join(departments_df,categories_df("category_department_id")===departments_df("department_id"))
      .select(departments_df("department_name").alias("depname"),
        year(orders_df("order_date")).alias("yr"),
        order_items_df("order_item_subtotal"))
      .groupBy("depname","yr")
      .agg(sum(order_items_df("order_item_subtotal")).alias("TotalRev"))
      .sort("depname","yr")
    //tempdf1.show(false)
    /*
    * +--------+----+------------------+
|depname |yr  |TotalRev          |
+--------+----+------------------+
|Apparel |2013|3228557.6381111145|
|Apparel |2014|4095142.8072624207|
|Fan Shop|2013|7615003.817440033 |
|Fan Shop|2014|9492762.462116241 |
|Fitness |2013|123587.89089012146|
|Fitness |2014|156456.25099945068|
|Footwear|2013|1789823.7386837006|
|Footwear|2014|2216675.0290584564|
|Golf    |2013|2058230.9001998901|
|Golf    |2014|2550797.3420562744|
|Outdoors|2013|438985.3807525635 |
|Outdoors|2014|556597.3408546448 |
+--------+----+------------------+*/
    //tempdf1.groupBy("depname").pivot("yr").avg("TotalRev").show(false)
    /*
    * +--------+------------------+------------------+
|depname |2013              |2014              |
+--------+------------------+------------------+
|Golf    |2058230.9001998901|2550797.3420562744|
|Apparel |3228557.6381111145|4095142.8072624207|
|Outdoors|438985.3807525635 |556597.3408546448 |
|Fitness |123587.89089012146|156456.25099945068|
|Footwear|1789823.7386837006|2216675.0290584564|
|Fan Shop|7615003.817440033 |9492762.462116241 |
+--------+------------------+------------------+*/

   System.out.println("Get Highest Priced Product:")
    spark.sql("select product_name,product_price from products where product_price = ( select max(product_price) from products ) ").show(false)
    products_df.select("*")
      .filter(col("product_price") === products_df.select(max("product_price")).first().get(0)/*.collect()(0)(0)*/)
    .show()
//    val max = df.agg(org.apache.spark.sql.functions.max(col("id"))).collect()(0)(0).asInstanceOf[Int]

    //using rank; hoghest per category
    spark.sql("select * from ( " +
      "select c.category_name,p.product_name,p.product_price, " +
      "( rank() over (PARTITION BY  c.category_name ORDER BY p.product_price desc) ) as RANKPRICE " +
      "from products p, categories c " +
      "where p.product_category_id=c.category_id) tmp " +
      "where RANKPRICE<=2 ").show(100,false)
    //using df api
    val winspec1 = Window.partitionBy("category_name").orderBy(products_df("product_price").desc)
    products_df.join(categories_df,products_df("product_category_id")===categories_df("category_id"))
      .select("category_name","product_name","product_price")
      .withColumn("RANKEDPRICE",rank().over(winspec1))
      .filter(col("RANKEDPRICE") <= 2).show(false)
    /*
    * +--------------------+---------------------------------------------+-------------+---------+
|category_name       |product_name                                 |product_price|RANKPRICE|
+--------------------+---------------------------------------------+-------------+---------+
|Training by Sport   |Manduka PRO Yoga Mat                         |100.0        |1        |
|Training by Sport   |Nike Women's Studio Wrap Three-Part Pack     |81.99        |2        |
|Men's Golf Clubs    |Merrell Men's Moab Rover Mid Waterproof Wide |139.99       |1        |
|Men's Golf Clubs    |Merrell Men's Moab Rover Mid Waterproof Wide |139.99       |1        |
|Men's Golf Clubs    |Merrell Men's Moab Rover Mid Waterproof Hikin|139.99       |1        |
|Camping & Hiking    |Diamondback Adult Trace Hybrid Bike 2014     |449.99       |1        |
|Camping & Hiking    |GoPro HERO3+ Black Edition Camera            |399.99       |2        |
|Camping & Hiking    |Diamondback Women's Clarity Hybrid Bike 2013 |399.99       |2        |
|Camping & Hiking    |Diamondback Adult Insight Hybrid Bike 2013   |399.99       |2        |
|Camping & Hiking    |Diamondback Women's Clarity 1 Hybrid Bike 201|399.99       |2        |
|Fitness Accessories |Marcy Diamond 9010 Smith Cage                |799.99       |1        |
|Fitness Accessories |Bowflex SelectTech 1090 Dumbbells            |599.99       |2        |*/

    System.out.println("Get Highest Revenue Earning Products")
    spark.sql("select p.product_name,sum(oi.order_item_subtotal) as totalProdRevenue from " +
      "order_items oi, products p, orders o where oi.order_item_product_id = p.product_id and " +
      "oi.order_item_order_id = o.order_id " +
      "group by p.product_name " +
      "order by totalProdRevenue desc").show(false)

     System.out.println("Top 5 Highest Revenue Earning Products Per Year:")
    spark.sql("select * from (select *,RANK() OVER (PARTITION BY yr ORDER BY totalProdRevenue desc) AS RANKEDTOPPROD " +
      "from (select year(o.order_date) AS yr ,p.product_name AS prdct ,sum(oi.order_item_subtotal) AS totalProdRevenue " +
      "from order_items oi, products p, orders o where " +
      "oi.order_item_product_id = p.product_id and " +
      "oi.order_item_order_id = o.order_id " +
      "group by yr,prdct " +
      "order by totalProdRevenue) tmp1 ) tmp2 " +
      "where RANKEDTOPPROD<=5").show(false)
    /*
    * |yr  |prdct                                        |totalProdRevenue  |RANKEDTOPPROD|
+----+---------------------------------------------+------------------+-------------+
|2013|Field & Stream Sportsman 16 Gun Fire Safe    |3076246.2644958496|1            |
|2013|Perfect Fitness Perfect Rip Deck             |1942416.2642097473|2            |
|2013|Diamondback Women's Serene Classic Comfort Bi|1832277.9071044922|3            |
|2013|Nike Men's Free 5.0+ Running Shoe            |1641435.8380355835|4            |
|2013|Nike Men's Dri-FIT Victory Golf Polo         |1393000.0         |5            |
|2014|Field & Stream Sportsman 16 Gun Fire Safe    |3853407.425842285 |1            |
|2014|Perfect Fitness Perfect Rip Deck             |2478726.8793144226|2            |
|2014|Diamondback Women's Serene Classic Comfort Bi|2286147.6637268066|3            |
|2014|Nike Men's Free 5.0+ Running Shoe            |2026197.3586273193|4            |
|2014|Nike Men's Dri-FIT Victory Golf Polo         |1754800.0         |5            |
+----+---------------------------------------------+------------------+-------------+*/

    order_items_df.join(orders_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .join(products_df,products_df("product_id")===order_items_df("order_item_product_id"))
      .select(year(orders_df("order_date")).alias("yr"),
        products_df("product_name").alias("prdct"),
        order_items_df("order_item_subtotal"))
      .groupBy("yr","prdct")
      .agg(sum(order_items_df("order_item_subtotal")).alias("totalProdRevenue"))
      .select("yr","prdct","totalProdRevenue")
      .withColumn("rankedyr",rank()
        .over(
          Window
            .partitionBy("yr")
            .orderBy(col("totalProdRevenue").desc))
      )
      .filter(col("rankedyr")<=5).show(false)

    System.out.println("Get the most popular Categories")
    spark.sql("select ct.category_name,count(*) AS cnt " +
      "from order_items oi, products p, orders o, categories ct where " +
      "oi.order_item_product_id = p.product_id and " +
      "oi.order_item_order_id = o.order_id and " +
      "p.product_category_id = ct.category_id " +
      "group by ct.category_name " +
      "order by cnt desc ").show(false)

      System.out.println("Get the revenue for each Category Per Year Per Quarter")
    spark.sql("select ct.category_name,year(o.order_date) AS yr ,concat('Q_',quarter(o.order_date)) as qtr, sum(oi.order_item_subtotal) AS totalRev " +
      "from order_items oi, products p, orders o, categories ct where " +
      "oi.order_item_product_id = p.product_id and " +
      "oi.order_item_order_id = o.order_id and " +
      "p.product_category_id = ct.category_id " +
      "group by ct.category_name,yr,qtr ").show(false)

    System.out.println("Get all CANCELED orders with amount greater than $1000")
    spark.sql("select * from (select o.order_id,o.order_date,sum(oi.order_item_subtotal) as totalperorder " +
      "from order_items oi, orders o where " +
      "oi.order_item_order_id = o.order_id and " +
      "o.order_status= 'CANCELED' " +
      "group by o.order_id,o.order_date)tmp " +
      "where totalperorder > 1000 " +
      "order by totalperorder desc").show(false)

     System.out.println("Sort Products by Category and Price")
      spark.sql("select c.category_name,p.product_name,p.product_price " +
        "from products p, categories c where " +
        "c.category_id = p.product_category_id " +
        "order by p.product_price DESC").show(false)

    System.out.println("Sort Products by Price within Each Category")
    spark.sql("select c.category_name,p.product_name,p.product_price " +
      "from products p, categories c where " +
      "c.category_id = p.product_category_id " +
      "distribute by c.category_name " +
      "sort by p.product_price" ).show(false)

    products_df.join(categories_df,products_df("product_category_id")===categories_df("category_id"))
      .select("category_name","product_name","product_price")
      .repartition(col("category_name"))
      .sortWithinPartitions("product_price").show(false)

    System.out.println("Get ‘topN priced’ products in each category")
    //distinctcly prices top 5
    spark.sql("select * from (select c.category_name,p.product_name,p.product_price, " +
      "dense_rank() over ( PARTITION BY c.category_name ORDER BY p.product_price DESC ) AS RNK " +
      "from products p, categories c where " +
      "c.category_id = p.product_category_id)tmp " +
      "where RNK<=5 ").show(false)

    products_df.join(categories_df,products_df("product_category_id")===categories_df("category_id"))
      .select("category_name","product_name","product_price")
      .withColumn("rnk",dense_rank().over(Window.partitionBy("category_name").orderBy(col("product_price").desc)))
      .filter("rnk<=5")
      .show(false)

    System.out.println("Get the top 3 Max Revenue Generating Customers Per year")
    spark.sql("select * from (select *, " +
      "dense_rank() OVER ( PARTITION BY yr ORDER BY totrev DESC ) AS rnk from " +
      "(select year(o.order_date) as yr,concat(cms.customer_fname,'_',cms.customer_lname) as name, " +
      "sum(oi.order_item_subtotal) as totrev " +
      "from orders o, order_items oi, customers cms " +
      "where oi.order_item_order_id=o.order_id and " +
      "o.order_customer_id=cms.customer_id " +
      "group by yr,name) tmp1 ) tmp2 " +
      "where rnk<=3 ").show(false)

    System.out.println("Get All Customers who didn’t place an order in 2013 from Oct to Dec")
    spark.sql("select * from customers " +
      "LEFT ANTI JOIN " +
      "(select distinct(o.order_customer_id) as cstid " +
      "from orders o, order_items oi " +
      "where oi.order_item_order_id=o.order_id and " +
      "date(o.order_date) BETWEEN '2013-10-01' AND '2013-12-31') tmp " +
      "ON customers.customer_id = tmp.cstid")

    //df having unique cust who placed orders between "2013-10-01" & "2013-12-31"
    val tmpdf = order_items_df.join(orders_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .filter(to_date(col("order_date")) >= "2013-10-01" &&  to_date(col("order_date")) <= "2013-12-31"  )
      .select(col("order_customer_id")).distinct()
    customers_df.join(tmpdf,customers_df("customer_id")===tmpdf("order_customer_id"),"leftanti")
      .select("*")

    System.out.println("Get All Customers id who placed an order in 2013 from Oct to Dec with 'min count of orders=20' ")
    spark.sql("select o.order_customer_id as cstid,count(o.order_customer_id) as ordrdcnt " +
      "from orders o, order_items oi " +
      "where oi.order_item_order_id=o.order_id and " +
      "date(o.order_date) BETWEEN '2013-10-01' AND '2013-12-31' " +
      "group by cstid " +
      "having ordrdcnt > 20 " +
      "order by ordrdcnt desc").show()

    order_items_df.join(orders_df, orders_df("order_id")===order_items_df("order_item_order_id"))
      .filter(to_date(col("order_date")) >= "2013-10-01" &&  to_date(col("order_date")) <= "2013-12-31"  )
      .select(col("order_customer_id").alias("cstid"))
      .groupBy("cstid").agg(count("cstid").alias("ordrcnt"))
      .where(col("ordrcnt")>20)
      .sort(col("ordrcnt").desc).show()

    spark.close()

  }

}
