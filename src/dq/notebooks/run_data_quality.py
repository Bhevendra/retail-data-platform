# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ============================================================
# 0. CONFIG PATH
# ============================================================

dbutils.widgets.text("path", "")

config_path = dbutils.widgets.get("path")


# ============================================================
# 1. IMPORTS
# ============================================================

import json
import os
import uuid

from common_utils.logging import get_logger
from common_utils.data_quality import (
    run_data_quality,
    create_dq_audit_table,
    write_dq_results,
    has_failures,
)


# ============================================================
# 2. LOGGER
# ============================================================

logger = get_logger("run_data_quality")


# ============================================================
# 3. READ CONFIG
# ============================================================

logger.info(
    "Reading Data Quality configuration from %s",
    config_path
)

with open(config_path, "r") as f:
    master_config = json.load(f)

logger.info(
    "Data Quality configuration loaded successfully"
)


# ============================================================
# 4. SOURCE / TARGET CONFIGURATION
# ============================================================

run_config = master_config["run"]

config_directory = os.path.dirname(config_path)

audit_table = run_config["audit_table"]

layer = run_config.get(
    "layer",
    "SILVER_GOLD"
)

fail_on_error = run_config.get(
    "fail_on_error",
    True
)

run_id = str(uuid.uuid4())


logger.info(
    "DQ run_id: %s",
    run_id
)

logger.info(
    "DQ audit table: %s",
    audit_table
)


# ============================================================
# 5. LOAD DQ CONFIGURATIONS
# ============================================================

logger.info(
    "Loading individual DQ configurations"
)

configs = []

for config_file in master_config["configs"]:

    dq_config_path = os.path.join(
        config_directory,
        config_file
    )

    logger.info(
        "Reading DQ config: %s",
        dq_config_path
    )

    with open(dq_config_path, "r") as f:
        configs.append(
            json.load(f)
        )


logger.info(
    "Loaded %s DQ configurations",
    len(configs)
)


# ============================================================
# 6. READ TABLES
# ============================================================

logger.info(
    "Reading tables required for DQ"
)

# ------------------------------------------------------------
# IMPORTANT:
#
# Do NOT use .cache() or .persist() here.
#
# Databricks Serverless does not support the persistence
# operation being used by the previous implementation.
# ------------------------------------------------------------

table_cache = {}

for dq_config in configs:

    table_name = dq_config["table"]

    logger.info(
        "Reading table: %s",
        table_name
    )

    if table_name not in table_cache:

        table_cache[table_name] = (
            spark.read
            .table(table_name)
        )


logger.info(
    "All DQ source tables loaded successfully"
)


# ============================================================
# 7. CREATE AUDIT SCHEMA / TABLE
# ============================================================

logger.info(
    "Creating audit schema if required"
)

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS
    retaildataplatform.audit
""")


create_dq_audit_table(
    spark,
    audit_table
)


# ============================================================
# 8. RUN DATA QUALITY
# ============================================================

all_results = []

for dq_config in configs:

    table_name = dq_config["table"]

    logger.info(
        "============================================================"
    )

    logger.info(
        "Starting DQ for %s",
        table_name
    )

    # --------------------------------------------------------
    # Get DataFrame without cache / persist
    # --------------------------------------------------------

    df = table_cache[table_name]

    results = run_data_quality(
        df,
        dq_config,
        table_cache
    )

    for result in results:

        result["layer"] = dq_config.get(
            "layer",
            layer
        )

    all_results.extend(
        results
    )

    logger.info(
        "Completed DQ for %s",
        table_name
    )


# ============================================================
# 9. WRITE DQ RESULTS
# ============================================================

logger.info(
    "Writing DQ results to %s",
    audit_table
)

write_dq_results(
    spark,
    all_results,
    audit_table,
    run_id,
    layer
)

logger.info(
    "DQ results written successfully"
)


# ============================================================
# 10. DISPLAY RESULTS
# ============================================================

dq_results_df = (
    spark.read
    .table(audit_table)
    .filter(
        f"run_id = '{run_id}'"
    )
    .orderBy(
        "table_name",
        "check_name"
    )
)


display(
    dq_results_df
)


# ============================================================
# 11. SUMMARY
# ============================================================

summary_df = (
    dq_results_df
    .groupBy("status")
    .count()
    .orderBy("status")
)

logger.info(
    "Data Quality summary:"
)

summary_df.show()


# ============================================================
# 12. FAILURE HANDLING
# ============================================================

failed_results = [
    result
    for result in all_results
    if result["status"] == "FAIL"
    and result["on_failure"] == "fail"
]

warn_results = [
    result
    for result in all_results
    if result["status"] == "WARN"
]


logger.info(
    "DQ failed checks: %s",
    len(failed_results)
)

logger.info(
    "DQ warning checks: %s",
    len(warn_results)
)


if fail_on_error and has_failures(
    all_results
):

    failed_names = [
        result["check_name"]
        for result in failed_results
    ]

    raise ValueError(
        "Data Quality failed. Failed checks: "
        + ", ".join(failed_names)
    )


logger.info(
    "Data Quality pipeline completed successfully"
)

# COMMAND ----------

