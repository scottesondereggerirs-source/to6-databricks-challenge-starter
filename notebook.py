# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/scott.e.sonderegger.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# DBTITLE 1,Outcome 1 summary: source and findings
# MAGIC %md
# MAGIC ### Source reference and findings
# MAGIC
# MAGIC **Source.** The inputs are in `training.to6_challenge3_inputs`. The supplied lineage view `source_lineage`, and the Catalog Explorer Lineage tab, show that both delivered batches, `returns_initial` and `returns_corrected`, come from the upstream table `parsed_source`. The transformation between them is not visible.
# MAGIC
# MAGIC **Row meaning.** One row is one tax return. `return_id` identifies the return and must be unique. `taxpayer_id` identifies the filer, who can have returns in several years. The required fields are `return_id`, `tax_year`, `income` and `deduction`. Amounts are nonnegative, and zero income is valid. A *batch* is one whole delivered table.
# MAGIC
# MAGIC **Whole-batch summaries** (produced by the cell below):
# MAGIC
# MAGIC | Table | Rows | 2022 / 2023 / 2024 | Rows missing a required value | Extra duplicate rows | Zero income |
# MAGIC | --- | --- | --- | --- | --- | --- |
# MAGIC | `parsed_source` (upstream) | 12 | 4 / 4 / 4 | 0 | 0 | R03, R09 |
# MAGIC | `returns_initial` | 13 | 5 / 4 / 4 | 1 (R02, `income`) | 1 (R01 twice) | R03, R09 |
# MAGIC | `returns_corrected` | 12 | 4 / 4 / 4 | 0 | 0 | R03, R09 |
# MAGIC
# MAGIC **What this changes for the next steps:**
# MAGIC * `returns_initial` breaks uniqueness and completeness, so the outcome 2 checks must reject it and keep the affected records (both copies of `R01`, and `R02`).
# MAGIC * `returns_corrected` matches `parsed_source` on every summary and should pass the same rules.
# MAGIC * `R03` and `R09` have zero income. They are valid returns, so they must stay in the features with a NULL ratio, and the division must not fail on them.

# COMMAND ----------

# DBTITLE 1,List tables and columns in input schema
#I brought in the prompts/clarifications from the powerpoint. Used the code below and manual browsing to help make the observatoins:
#Prompt 1 Catalog descriptions, three sample rows, the lineage view; a concise source reference and explanation; A note: row, fields, identifier, source
#Answer 1 -- there are four tables: parsed_source, returns_initial, returns_corrected, source_lineage. source_lineage and the Catalog Lineage tab both show parsed_source as the upstream source of the two delivered batches, returns_initial and returns_corrected. The transformation code between them is hidden. 

#3 Inspect the full batch as well as the sample rows; a batch is one whole delivered table (returns_initial or returns_corrected). returns_initial has 13 records, one more than its upstream parsed_source (12): R01 appears twice, and R02 has a null income that parsed_source does not have, so the hidden delivery step introduced these defects. 

"""+---------+-----------+--------+------+---------+
|return_id|taxpayer_id|tax_year|income|deduction|
+---------+-----------+--------+------+---------+
|      R01|         F0|    2022|   100|       20|
|      R02|         F1|    2022|  NULL|       33|
|      R03|         F2|    2022|     0|       46|
|      R04|         F3|    2022|   400|       59|
|      R05|         F0|    2023|   500|       72|
|      R06|         F1|    2023|   600|       85|
|      R07|         F2|    2023|   700|       98|
|      R08|         F3|    2023|   800|      111|
|      R09|         F0|    2024|     0|      124|
|      R10|         F1|    2024|  1000|      137|
|      R11|         F2|    2024|  1100|      150|
|      R12|         F3|    2024|  1200|      163|
|      R01|         F0|    2022|   100|       20|
+---------+-----------+--------+------+---------+"""
#Prompt 4 Summarize returns by year, missing required values, duplicate return IDs and zero-income records; Four summaries, whole batch
# Answer 4: The full data is in the code output below, but the basic takeaway agrees with the visual observations. In returns_initial, there is an extra record for 2022, with a duplicate return_id and a null value for income. Again, extra odd, since this supposedly is downstream from parsed_source:
"""--- Summaries for returns_initial ---

  Records by year:
+--------+-----+
|tax_year|count|
+--------+-----+
|    2022|    5|
|    2023|    4|
|    2024|    4|
+--------+-----+

  Missing (null) values per column:
+---------+-----------+--------+------+---------+
|return_id|taxpayer_id|tax_year|income|deduction|
+---------+-----------+--------+------+---------+
|        0|          0|       0|     1|        0|
+---------+-----------+--------+------+---------+

  Duplicate return_ids:
+---------+---+
|return_id|cnt|
+---------+---+
|      R01|  2|
+---------+---+
"""

#Prompt 5 Keep the queries and results in the notebook, and explain which findings affect your next checks or transformations; Found, and what it changes. A chart is optional. These observations do not replace the quality checks in the next step; Outcome 2's checks still decide
#Answer 5: Looks like returns_initial has a bad record, so we're expecting that when we're done with the processing, it should be gone. 

# Your investigation here.
# Set the schema
schema_name = "training.to6_challenge3_inputs"

# Get all tables in the schema
tables = spark.catalog.listTables(schema_name)
 
for table in tables:
    print(f"\nTable: {table.name}")
    print(f"  Comment: {table.description or '(none)'}")
    # Get columns for this table
    columns = spark.catalog.listColumns(f"{schema_name}.{table.name}")
    # Let's see if I can figure out a count
    df = spark.table(f"{schema_name}.{table.name}")
    print(f"  Record Count: {df.count()}")
    #Summarize returns by year, missing required values, duplicate return IDs and zero-income records; Four summaries, whole batch
    if table.name in ("parsed_source", "returns_initial", "returns_corrected"):
        print(f"\n--- Summaries for {table.name} ---")
        #
        # 1. Records by year
        print("\n  Records by year:")
        df.groupBy("tax_year").count().orderBy("tax_year").show()
        #
        # 2. Missing required values (nulls per column)
        from pyspark.sql.functions import col, sum as _sum, lit
        print("  Missing (null) values per column:")
        df.select([_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
        #
        # 3. Duplicate return_ids
        from pyspark.sql.functions import count
        print("  Duplicate return_ids:")
        df.groupBy("return_id").agg(count("*").alias("cnt")).filter("cnt > 1").orderBy("return_id").show()
        #
        # 4. Zero-income records
        from pyspark.sql.functions import col as _col
        print("  Zero-income records:")
        df.filter(_col("income") == 0).show()
    if 0 < df.count() < 30 :
        df.show()
    for col in columns:
        print(f"    {col.name} ({col.dataType}) - {col.description or '(no comment)'}")

#Pasting in the output as comments, because it's suppposed to be in normal language


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,Validate batches and choose good_batch_df
# Your checks and decision here.
# Each batch is one whole delivered table. Both are checked with the same rules: completeness (no null
# return_id, tax_year, income or deduction), uniqueness (return_id appears once) and nonnegative amounts.
# Result: returns_initial is REJECTED. R01 appears twice (uniqueness) and R02 has a null income (completeness).
# Those 3 records and their reasons are saved to batch_rejections; no row is dropped, de-duplicated or zero-filled.
# Next action: returns_corrected passes the same rules (0 failing records), so it is the accepted batch
# (good_batch_df) and the only source of the feature output.

from pyspark.sql import functions as F, Window

# Data contract rules from CHALLENGE.md
REQUIRED = ["return_id", "tax_year", "income", "deduction"]
AMOUNTS = ["income", "deduction"]

def find_violations(df):
    """Return every record that breaks a rule, with a rejection_reason column."""
    dup_count = F.count(F.lit(1)).over(Window.partitionBy("return_id"))
    checks = (
        [(F.col(c).isNull(), f"completeness: {c} is null") for c in REQUIRED]
        + [(dup_count > 1, "uniqueness: return_id appears more than once")]
        + [(F.col(c) < 0, f"nonnegative: {c} is negative") for c in AMOUNTS]
    )
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(msg)) for cond, msg in checks]))
    return (df.withColumn("_reasons", reasons)
              .filter(F.size("_reasons") > 0)
              .withColumn("rejection_reason", F.array_join("_reasons", "; "))
              .drop("_reasons"))

# Check each batch in delivery order; the first batch with no violations is accepted.
batches = [INITIAL_TABLE, CORRECTED_TABLE]
all_violations = None
good_batch_table = None
for table in batches:
    violations = find_violations(spark.table(table)).withColumn("batch", F.lit(table))
    n = violations.count()
    print(f"{table}: {spark.table(table).count()} rows, {n} violating records -> {'REJECTED' if n else 'ACCEPTED'}")
    all_violations = violations if all_violations is None else all_violations.unionByName(violations)
    if n == 0 and good_batch_table is None:
        good_batch_table = table

# Preserve the affected records and reasons (overwrite, so reruns do not append copies).
REJECTIONS_TABLE = f"{OUTPUT_SCHEMA}.batch_rejections"
all_violations.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTIONS_TABLE)
display(spark.table(REJECTIONS_TABLE).orderBy("batch", "return_id"))

# Stop before any feature output if no batch passes.
assert good_batch_table is not None, "No batch passed the completeness and uniqueness checks"
good_batch_df = spark.table(good_batch_table)
print("good_batch_df =", good_batch_table)





# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build and save return_features
# Build return_features from whichever batch cell 8 accepted (good_batch_table / good_batch_df).
from pyspark.sql.functions import col, try_divide

FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
FEATURE_COLUMNS = ["return_id", "tax_year", "deduction_to_income"]
print(f"Source batch: {good_batch_table}")

# Ratio is deduction / income; a zero-income return keeps its row with a NULL ratio.
features_df = good_batch_df.select(
    col("return_id"),
    col("tax_year"),
    try_divide(col("deduction"), col("income")).alias("deduction_to_income"),
)

# overwrite removes every existing row (and replaces schema/partitioning) in one Delta transaction:
# nothing from an earlier run survives, and a failed write leaves the previous version intact.
(features_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("tax_year")
    .saveAsTable(FEATURES_TABLE))

# Read back: exactly the three columns, and exactly the accepted returns, once each.
saved = spark.table(FEATURES_TABLE)
accepted_ids = good_batch_df.select("return_id")
saved_ids = saved.select("return_id")
assert saved.columns == FEATURE_COLUMNS, f"unexpected columns: {saved.columns}"
assert saved.count() == good_batch_df.count(), f"{saved.count()} rows saved, {good_batch_df.count()} accepted"
assert saved_ids.distinct().count() == saved.count(), "a return_id appears more than once"
assert saved_ids.exceptAll(accepted_ids).count() == 0, "the table holds a return that is not in the accepted batch"
assert accepted_ids.exceptAll(saved_ids).count() == 0, "an accepted return is missing from the table"

print(f"{FEATURES_TABLE}: {saved.count()} rows, one per accepted return")
saved.orderBy("tax_year", "return_id").show()

# Expected nonzero value, derived by hand from the source row (not from Spark):
# R01 in the accepted batch has deduction 20 and income 100, so deduction_to_income = 20 / 100 = 0.2.
EXPECTED_ID, EXPECTED_DEDUCTION, EXPECTED_INCOME, EXPECTED_RATIO = "R01", 20, 100, 0.2
source = spark.table(good_batch_table)
src = source.filter(col("return_id") == EXPECTED_ID).select("income", "deduction").collect()
assert len(src) == 1, f"{EXPECTED_ID} appears {len(src)} times in the source"
assert (src[0]["income"], src[0]["deduction"]) == (EXPECTED_INCOME, EXPECTED_DEDUCTION), \
    f"source values for {EXPECTED_ID} changed: {src[0].asDict()}"
got = saved.filter(col("return_id") == EXPECTED_ID).collect()[0]["deduction_to_income"]
assert got is not None and abs(got - EXPECTED_RATIO) < 1e-12, f"{EXPECTED_ID}: saved {got}, expected {EXPECTED_RATIO}"
print(f"Expected-value check: {EXPECTED_ID} = {EXPECTED_DEDUCTION} / {EXPECTED_INCOME} = {EXPECTED_RATIO}; saved table has {got}")

# Zero-income check, on the saved table: every zero-income source return is present once, with a NULL ratio.
zero_ids = sorted(r["return_id"] for r in source.filter(col("income") == 0).select("return_id").collect())
assert zero_ids, "the source has no zero-income return to check"
zero_saved = {r["return_id"]: r["deduction_to_income"]
              for r in saved.filter(col("return_id").isin(zero_ids)).collect()}
assert sorted(zero_saved) == zero_ids, f"zero-income returns missing from the table: {set(zero_ids) - set(zero_saved)}"
assert all(v is None for v in zero_saved.values()), f"a zero-income return has a non-null ratio: {zero_saved}"
print(f"Zero-income check: {', '.join(zero_ids)} are in the saved table with a NULL ratio")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Save year split tables
# Your implementation, rerun comparison and exports here.
# Year split tables, built from the SAVED return_features table (not from features_df).
from functools import reduce
from pyspark.sql.functions import col

SPLIT_YEARS = {"features_train": 2022, "features_validation": 2023, "features_test": 2024}
saved_features = spark.table(FEATURES_TABLE)

# overwrite: each run replaces the split tables, so reruns leave no appended copies.
for name, year in SPLIT_YEARS.items():
    (saved_features.filter(col("tax_year") == year)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{OUTPUT_SCHEMA}.{name}"))

# Read back each split: only its assigned year, same columns as return_features.
splits = {name: spark.table(f"{OUTPUT_SCHEMA}.{name}") for name in SPLIT_YEARS}
for name, year in SPLIT_YEARS.items():
    years = sorted(r[0] for r in splits[name].select("tax_year").distinct().collect())
    assert years == [year], f"{name} holds tax years {years}, expected only {year}"
    assert splits[name].columns == saved_features.columns, f"{name} columns differ: {splits[name].columns}"
    print(f"{OUTPUT_SCHEMA}.{name}: {splits[name].count()} rows, tax year {year}")

# Every saved feature row is in exactly one split, with the same values.
combined = reduce(lambda a, b: a.unionByName(b), splits.values())
assert combined.count() == saved_features.count(), \
    f"{combined.count()} rows across splits, {saved_features.count()} in return_features (a year is unassigned?)"
assert combined.select("return_id").distinct().count() == combined.count(), "a return_id is in more than one split"
assert combined.exceptAll(saved_features).count() == 0, "a split holds a row not in return_features"
assert saved_features.exceptAll(combined).count() == 0, "a return_features row is missing from the splits"
print("Every accepted return is in exactly one split, in its assigned year, with unchanged values.")

# COMMAND ----------

# DBTITLE 1,Export final tables and rejections to CSV
# Small CSV exports for the submission, read from the SAVED tables and overwritten on every run.
import pandas as pd

EXPORTS = {
    "return_features": ["return_id"],
    "features_train": ["return_id"],
    "features_validation": ["return_id"],
    "features_test": ["return_id"],
    "batch_rejections": ["batch", "return_id", "rejection_reason"],  # affected initial records + reasons
}
export_dir = repo_root / "outputs"
export_dir.mkdir(exist_ok=True)

for table, sort_cols in EXPORTS.items():
    df = spark.table(f"{OUTPUT_SCHEMA}.{table}").orderBy(*sort_cols).toPandas().convert_dtypes()  # keeps 100 as 100 when a column has a NULL
    path = export_dir / f"{table}.csv"
    df.to_csv(path, index=False)  # NULL ratios are written as empty fields
    back = pd.read_csv(path)
    assert len(back) == len(df) and list(back.columns) == list(df.columns), f"{path.name} did not read back intact"
    print(f"Wrote {path.relative_to(repo_root)}: {len(df)} rows, columns {list(df.columns)}")

# COMMAND ----------

# DBTITLE 1,Save this run's snapshot
# Run snapshot: written only when every earlier cell succeeded, so each file is one complete successful run.
import json
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient

SNAPSHOT_TABLES = ["return_features", "features_train", "features_validation", "features_test"]
RUNS_DIR = repo_root / "outputs" / "runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

run_time = datetime.now(timezone.utc)
notebook_path = str(repo_root / "notebook").removeprefix("/Workspace")
try:
    notebook_id = WorkspaceClient().workspace.get_status(notebook_path).object_id
    host = spark.conf.get("spark.databricks.workspaceUrl")
    notebook_url = f"https://{host}/editor/notebooks/{notebook_id}"
except Exception as e:  # URL is evidence only; record why it is missing rather than fail the run
    notebook_url = f"unavailable ({type(e).__name__}); add it to SUBMISSION.md by hand"

snapshot = {
    "executed_at_utc": run_time.isoformat(timespec="seconds"),
    "notebook_path": notebook_path,
    "notebook_url": notebook_url,
    "accepted_batch": good_batch_table,
    "tables": {
        t: [r.asDict() for r in spark.table(f"{OUTPUT_SCHEMA}.{t}").orderBy("return_id").collect()]
        for t in SNAPSHOT_TABLES
    },
}
snapshot_file = RUNS_DIR / f"run_{run_time.strftime('%Y%m%dT%H%M%SZ')}.json"
snapshot_file.write_text(json.dumps(snapshot, indent=1) + "\n", encoding="utf-8")
print("Wrote", snapshot_file.relative_to(repo_root), "|", snapshot["executed_at_utc"], "|", notebook_url)
for t, rows in snapshot["tables"].items():
    print(f"  {t}: {len(rows)} rows")

# COMMAND ----------

# DBTITLE 1,Compare the two most recent runs
# Compare the two most recent run snapshots: same return IDs and same values in every table.
snapshots = sorted(RUNS_DIR.glob("run_*.json"))  # names sort by UTC timestamp
if len(snapshots) < 2:
    print(f"{len(snapshots)} run snapshot so far. Detach & re-attach, then Run all again for the second run.")
else:
    first_file, second_file = snapshots[-2], snapshots[-1]
    first, second = (json.loads(p.read_text(encoding="utf-8")) for p in (first_file, second_file))

    def as_rows(rows):
        return {r["return_id"]: (r["tax_year"], r["deduction_to_income"]) for r in rows}

    tables = {}
    for t in SNAPSHOT_TABLES:
        a, b = as_rows(first["tables"][t]), as_rows(second["tables"][t])
        tables[t] = {
            "rows_run_1": len(first["tables"][t]),
            "rows_run_2": len(second["tables"][t]),
            "only_in_run_1": sorted(a.keys() - b.keys()),
            "only_in_run_2": sorted(b.keys() - a.keys()),
            "value_differences": sorted(k for k in a.keys() & b.keys() if a[k] != b[k]),
        }
        tables[t]["match"] = (tables[t]["rows_run_1"] == tables[t]["rows_run_2"] == len(a) == len(b)
                              and not (tables[t]["only_in_run_1"] or tables[t]["only_in_run_2"]
                                       or tables[t]["value_differences"]))

    comparison = {
        "run_1": {"file": str(first_file.relative_to(repo_root)), "executed_at_utc": first["executed_at_utc"],
                  "notebook_url": first["notebook_url"]},
        "run_2": {"file": str(second_file.relative_to(repo_root)), "executed_at_utc": second["executed_at_utc"],
                  "notebook_url": second["notebook_url"]},
        "tables": tables,
        "all_match": all(v["match"] for v in tables.values()),
    }
    (repo_root / "outputs" / "run_comparison.json").write_text(json.dumps(comparison, indent=1) + "\n", encoding="utf-8")
    for t, v in tables.items():
        print(f"{t}: run 1 {v['rows_run_1']} rows, run 2 {v['rows_run_2']} rows -> {'MATCH' if v['match'] else 'DIFFERENT'}")
    print("Wrote outputs/run_comparison.json")
    assert comparison["all_match"], f"Runs differ: {json.dumps(tables, indent=1)}"
    print(f"Runs {first['executed_at_utc']} and {second['executed_at_utc']} left identical return IDs and values.")