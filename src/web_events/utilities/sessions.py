from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def add_session_id(df: DataFrame) -> DataFrame:
    """30-minute inactivity sessions per user, plus the user's region from the customer dimension."""
    spark = SparkSession.getActiveSession()
    customers = spark.read.table("silver.dim_customer").select(F.col("customer_id").alias("user_id"), "region")
    w = Window.partitionBy("user_id").orderBy("event_ts")
    gap = (F.col("event_ts").cast("long") - F.lag("event_ts").over(w).cast("long")) > 1800
    return (
        df.withColumn("new_session", F.coalesce(gap, F.lit(True)).cast("int"))
        .withColumn("session_no", F.sum("new_session").over(w))
        .withColumn("session_id", F.concat_ws("-", "user_id", "session_no"))
        .withColumn("event_date", F.to_date("event_ts"))
        .join(customers, "user_id", "left")
        .drop("new_session", "session_no")
    )
