import json

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from utilities.sessions import add_session_id

CHANNELS = ["web", "app"]
REGIONS = ["emea", "apac"]
FEEDS = json.loads(spark.conf.get("factory.feeds"))


@dp.table(name="bronze.web_events_raw", comment="Raw click-stream events")
def web_events_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(spark.conf.get("factory.landing_root") + "/web_events")
    )


dp.create_streaming_table(
    name="silver.events_all",
    comment="Events from all channels",
)

for channel in CHANNELS:
    @dp.append_flow(target="silver.events_all", name=f"events_from_{channel}")
    def events_from_channel(channel=channel):
        return spark.readStream.table("bronze.web_events_raw").where(F.col("channel") == channel)


@dp.materialized_view(name="silver.sessions", comment="Events of known users with session id and region")
def sessions():
    return add_session_id(spark.sql("SELECT * FROM silver.events_all WHERE user_id IS NOT NULL"))


for region in REGIONS:
    @dp.materialized_view(name=f"gold.events_daily_{region}", comment="Daily events and sessions for one region")
    def events_daily(region=region):
        return (
            spark.read.table("silver.sessions")
            .where(F.col("region") == region.upper())
            .groupBy("event_date", "event_type")
            .agg(F.count("*").alias("events"), F.countDistinct("session_id").alias("sessions"))
        )


def kpi_table(name, event_type):
    @dp.materialized_view(name=name, comment="Daily count of one event type")
    def kpi():
        return spark.sql(
            f"SELECT event_date, count(*) AS events FROM silver.sessions WHERE event_type = '{event_type}' GROUP BY event_date"
        )

    return kpi


kpi_table("gold.daily_checkouts", "checkout")
kpi_table("gold.daily_purchases", "purchase")

for feed in FEEDS:
    @dp.materialized_view(name=feed["table"], comment="Event feed shared with a downstream team")
    def feed_table(feed=feed):
        return spark.read.table("silver.sessions").where(F.col("event_type").isin(feed["event_types"]))
