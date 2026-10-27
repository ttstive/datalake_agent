# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (business aggregates + pipeline health)
# MAGIC
# MAGIC Batch SQL, fully recomputed every run from silver (data is small, so this is simple and always consistent).
# MAGIC SQL lives in `orders_gold.py`. Each table: `CREATE TABLE IF NOT EXISTS` (typed columns + comments) once,
# MAGIC then an atomic `INSERT OVERWRITE` every run.
# MAGIC
# MAGIC | table | grain |
# MAGIC |---|---|
# MAGIC | `gold_orders_per_minute` | one row per minute of `event_time` |
# MAGIC | `gold_revenue_by_category` | one row per category |
# MAGIC | `gold_status_funnel` | one row per status (5) |
# MAGIC | `gold_pipeline_health` | one row per pipeline step (8), rebuilt each run |
# MAGIC | `gold_pipeline_health_history` | append-only: 8 rows per run, keyed by `measured_at` |
# MAGIC
# MAGIC No streaming query here, so no checkpoint.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")  # noqa: F821
dbutils.widgets.text("schema", "ecommerce")  # noqa: F821

catalog = dbutils.widgets.get("catalog").strip()  # noqa: F821
schema = dbutils.widgets.get("schema").strip()  # noqa: F821

import os
import sys

# orders_gold.py sits next to this notebook (workspace file synced by the bundle).
sys.path.insert(0, os.getcwd())
import orders_gold as gold

tables = gold.fq_tables(catalog, schema)
history_table = f"{catalog}.{schema}.{gold.HEALTH_HISTORY_NAME}"
print("\n".join(f"{k} -> {v}" for k, v in tables.items()))

# COMMAND ----------

for t in gold.GOLD_TABLES:
    spark.sql(gold.create_sql(t, tables[t.name]))  # noqa: F821
spark.sql(gold.history_create_sql(history_table))  # noqa: F821

# COMMAND ----------

# Order matters: pipeline health counts the other gold tables, so it is rebuilt last.
for t in gold.GOLD_TABLES:
    spark.sql(gold.insert_overwrite_sql(t, tables[t.name], tables))  # noqa: F821
    print(f"{tables[t.name]}: {spark.table(tables[t.name]).count()} rows")  # noqa: F821

spark.sql(gold.history_append_sql(history_table, tables["gold_pipeline_health"]))  # noqa: F821
print(f"{history_table}: {spark.table(history_table).count()} rows")  # noqa: F821

# COMMAND ----------

display(  # noqa: F821
    spark.table(tables["gold_pipeline_health"]).orderBy("step_order")  # noqa: F821
)
