"""dq-agent: dataset-level DQX checks (uniqueness, referential integrity, volume, consistency) run after
each pipeline update. Checks are loaded from <ops_catalog>.dq.dqx_checks; results to <ops_catalog>.dq.results.
DQX API names verified against the pinned version (V-DQX) before use."""
import argparse
import json

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession, functions as F


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--ops-catalog", required=True)
    p.add_argument("--pipeline-key", required=True)
    p.add_argument("--catalog", required=True)
    a = p.parse_args()
    spark = SparkSession.builder.getOrCreate()
    engine = DQEngine(WorkspaceClient())
    checks = spark.read.table(f"{a.ops_catalog}.dq.dqx_checks").where(
        (F.col("pipeline_key") == a.pipeline_key) & (F.col("status") == "approved"))
    for dataset in [r["dataset"] for r in checks.select("dataset").distinct().collect()]:
        rows = [r.asDict(recursive=True)["check_json"] for r in checks.where(F.col("dataset") == dataset).collect()]
        metadata = [json.loads(x) for x in rows]
        df = spark.read.table(f"{a.catalog}.{dataset}")
        _, invalid = engine.apply_checks_by_metadata_and_split(df, metadata)
        failed, total = invalid.count(), df.count()
        (spark.createDataFrame([(a.pipeline_key, dataset, total - failed, failed)],
                               "pipeline_key string, dataset string, pass_count long, fail_count long")
         .withColumn("engine", F.lit("dqx")).withColumn("observed_at", F.current_timestamp())
         .write.mode("append").saveAsTable(f"{a.ops_catalog}.dq.results"))


if __name__ == "__main__":
    main()
