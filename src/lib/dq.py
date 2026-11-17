"""dq-agent runtime helper: returns {rule_id: sql_expression} for SDP expectation decorators.

Reads APPROVED rules for one dataset from <ops_catalog>.dq.rules. Pure and cheap: pipeline source
may be evaluated several times during planning. An empty dict is valid (decorator becomes a no-op).
"""

_STATUS_BY_LEVEL = {
    "warn": ("approved_warn",),
    "drop": ("approved_drop",),
    "fail": ("approved_fail",),
}


def rules_for(spark, dataset: str, level: str) -> dict:
    if level not in _STATUS_BY_LEVEL:
        raise ValueError(f"unknown level {level!r}")
    ops = spark.conf.get("dq.ops_catalog")
    pipeline = spark.conf.get("dq.pipeline_key")
    statuses = ",".join("'" + s + "'" for s in _STATUS_BY_LEVEL[level])
    rows = (
        spark.read.table(f"{ops}.dq.rules")
        .where(f"engine = 'sdp_expectation' AND pipeline_key = '{pipeline}' AND dataset = '{dataset}' AND status IN ({statuses})")
        .select("rule_id", "sdp_expression")
        .collect()
    )
    return {r["rule_id"]: r["sdp_expression"] for r in rows}
