# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from course.learner_42.test;

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

# DBTITLE 1,List tables and columns in input schema
#I brought in the prompts/clarifications from the powerpoint. Used the code below and manual browsing to help make the observatoins:
#Prompt 1 Catalog descriptions, three sample rows, the lineage view; a concise source reference and explanation; A note: row, fields, identifier, source
#Answer 1 -- there are four tables: parsed_source, returns_initial, returns_corrected, source_lineage. Parsed source and returns_initial get processed into returns_initial? Then parse_source and returns_corrected get processed into returns_corrected. Honestly, the picture really only clarifies that parsed_source is the starting point. The transformation code is hidden. 

#3 Inspect the full batch as well as the sample rows; Based on table comments and the diagram, I'm assuming that this means parsed_source, which somehow has one LESS record than its downstream returns_initial, which has 13 records. That appears to be an issue. Returns_initial has a row with a null column, parsed_source doesn't. Kinda seems like our post-processing is worse than the batch. 

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
#Answer 5: Looks like returns_initial has a bad record, so we're expecting that when we're done with whatever the hell we're doing, it should be gone. 

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

# Your checks and decision here.
# I guess, check the powerpoint for the instructions? God damnit. What in the hell is going on here? What is the definition of a "batch?" Are the different tables considered as different batches? Where are the fucking "batches?"

#ok -- half of the instructions make it look like I should be writing code here, but maybe we're still just doing visual observations? I've beaten this to death by now just trying to understand and work through the directions.
#Based on everything we're looking at, if we assume "batch" means an entire table, which nobody in their right mind would do, then it looks like parsed_source was used to create a "bad" batch in returns_initial and then a "good" batch in "returns_corrected"
# So, I guess "returns_corrected" is the correct batch, and "returns_initial" is the bad batch, based on the duplcate return_id with a null income value.



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# Your implementation and verification here.
#And what the hell are we talking about here? Certainly nothing in the instructions. I guess we'll look at the powerpoint, because hunding across a zillion documents is super helpful. Oh fuck, am I supposed to push to github again? 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.