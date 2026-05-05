


from pyspark import pipelines as dp



@dp.table(name="customers")
def bronze_customers():
    return spark.read.csv(
        "/Volumes/declarative/default/sales2026/customers/",
        header=True,
        inferSchema=True
    )

@dp.table(name="products")
def bronze_products():
    return spark.read.csv(
        "/Volumes/declarative/default/sales2026/products/",
        header=True,
        inferSchema=True
    )

@dp.table(name="orders")
def bronze_orders():
    return spark.read.csv(
        "/Volumes/declarative/default/sales2026/orders/",
        header=True,
        inferSchema=True
    )


----------------------

from pyspark import pipelines as dp


@dp.materialized_view(name="sales")
def silver_sales():
    
    customers = spark.read.table("bronze.customers")
    products  = spark.read.table("bronze.products")
    orders    = spark.read.table("bronze.orders")

    df = orders \
        .join(customers, "customer_id") \
        .join(products, "product_id") \
        .select(
            "order_id",
            "customer_id",
            customers["name"].alias("customer_name"),
            "city",
            "product_id",
            products["name"].alias("product_name"),
            "category",
            "price",
            "quantity",
            "order_date"
        )

    return df


-------------------------------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(name="daily_sales")
def gold_daily_sales():
    
    df = spark.read.table("silver.sales")

    return (
        df.withColumn("date", F.to_date("order_date"))
          .withColumn("year", F.year("date"))
          .withColumn("month", F.month("date"))
          .withColumn("total_value", F.col("quantity") * F.col("price"))
          .groupBy("date", "year", "month")
          .agg(F.sum("total_value").alias("revenue"))
    )