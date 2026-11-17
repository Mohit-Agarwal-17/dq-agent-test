from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lib.dq import rules_for  # dq-agent: rules are data in <ops_catalog>.dq.rules (status-filtered)


@dp.table(name="bronze.orders_raw", comment="Raw orders landed from source")
@dp.expect_all(rules_for(spark, "bronze.orders_raw", "warn"))  # dq-agent
def orders_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("rescuedDataColumn", "_rescued_data")
        .load(spark.conf.get("factory.landing_root") + "/orders/")
    )


@dp.materialized_view(name="silver.orders", comment="Latest version of each order")
@dp.expect_all(rules_for(spark, "silver.orders", "warn"))  # dq-agent
@dp.expect_all_or_drop(rules_for(spark, "silver.orders", "drop"))  # dq-agent: empty until rules graduate (CT-10)
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
@dp.expect_all(rules_for(spark, "gold.orders_daily", "warn"))  # dq-agent
def orders_daily():
    return (
        spark.read.table("silver.orders")
        .join(spark.read.table("silver.dim_customer"), on=["customer_id"], how="left")
        .withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date", "region")
        .agg(F.sum("amount").alias("revenue"), F.countDistinct("order_id").alias("orders"))
    )
