from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.table(name="bronze.orders_raw", comment="Raw orders landed from source")
def orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("rescuedDataColumn", "_rescued_data")
        .load(spark.conf.get("factory.landing_root") + "/orders/")
    )


@dp.materialized_view(name="silver.orders", comment="Latest version of each order")
def orders():
    w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    return (
        spark.read.table("bronze.orders_raw")
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .drop("_rn")
        .select("order_id", "customer_id", "order_ts", "updated_at", "status", "amount",
                "currency", "discount_pct", "ship_country", "customer_email")
    )


@dp.materialized_view(name="gold.orders_daily", comment="Daily revenue by region")
def orders_daily():
    return (
        spark.read.table("silver.orders")
        .join(spark.read.table("silver.dim_customer"), on=["customer_id"], how="left")
        .withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date", "region")
        .agg(F.sum("amount").alias("revenue"), F.countDistinct("order_id").alias("orders"))
    )
