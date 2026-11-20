# My dataset submission

## Inputs and outputs

**Inputs (read-only)**

| Role | Table |
| --- | --- |
| Initial batch (rejected) | `training.to6_challenge3_inputs.returns_initial` (13 rows) |
| Replacement batch (accepted) | `training.to6_challenge3_inputs.returns_corrected` (12 rows) |
| Supplied lineage view | `training.to6_challenge3_inputs.source_lineage` |
| Upstream source named by the lineage view | `training.to6_challenge3_inputs.parsed_source` (12 rows) |

`source_lineage` maps both delivered tables to the same upstream table, `parsed_source`
("initial delivery transformation; validation required" and "corrected delivery; same
validation rules"). I also checked the Catalog Explorer Lineage tab, which likewise shows
`parsed_source` upstream of both `returns_initial` and `returns_corrected`. A "batch" in this task
is one whole delivered table.

**Row meaning.** One row is one tax return. `return_id` identifies the return and must be unique;
`taxpayer_id` identifies the filer, who can have returns in several years. Required fields are
`return_id`, `tax_year`, `income` and `deduction`. Amounts are nonnegative; zero income is valid.

**Outputs (schema `training.to6_c3_57219e23e40e`)**

| Table | Rows | Contents |
| --- | --- | --- |
| `training.to6_c3_57219e23e40e.return_features` | 12 | `return_id`, `tax_year`, `deduction_to_income`; partitioned by `tax_year` |
| `training.to6_c3_57219e23e40e.features_train` | 4 | tax year 2022 (R01-R04) |
| `training.to6_c3_57219e23e40e.features_validation` | 4 | tax year 2023 (R05-R08) |
| `training.to6_c3_57219e23e40e.features_test` | 4 | tax year 2024 (R09-R12) |
| `training.to6_c3_57219e23e40e.batch_rejections` | 3 | affected initial records with `rejection_reason` and `batch` |

**Ratio definition.** `deduction_to_income = deduction / income`, computed with `try_divide`, so a
zero-income return stays in the table with a NULL ratio. Under the need-to-know rule the feature
and split tables exclude `taxpayer_id`, `income` and `deduction`.

## Checks and decisions

**Source references.** Notebook outcome 1 (cell "List tables and columns in input schema") records
the Catalog comment of each input table, sample rows, the lineage view, and whole-batch summaries:
returns by year, NULLs per column, duplicate `return_id`s and zero-income records. Findings that
shaped the checks: `returns_initial` has 5 rows for 2022 instead of 4, a duplicated `R01` and a
NULL `income` for `R02`; zero-income returns `R03` and `R09` are valid and need a NULL ratio.

**Rules (notebook outcome 2, cell "Validate batches and choose good_batch_df").** Each record is
checked for completeness (no NULL in the four required fields), uniqueness (`return_id` appears
once) and nonnegative `income`/`deduction`. The batches are checked in delivery order and the
first one with no failing record is accepted. If neither passes, an `assert` stops the run before
any feature table is written.

**Initial rejection.** `returns_initial`: 13 rows, 3 failing records, REJECTED.

| return_id | income | deduction | Reason |
| --- | --- | --- | --- |
| R01 (copy 1) | 100 | 20 | uniqueness: return_id appears more than once |
| R01 (copy 2) | 100 | 20 | uniqueness: return_id appears more than once |
| R02 | NULL | 33 | completeness: income is null |

The records are kept in `training.to6_c3_57219e23e40e.batch_rejections` and exported to
`outputs/batch_rejections.csv`. Next action: no row was dropped, de-duplicated or set to zero;
the whole batch was set aside and the replacement was assessed instead.

**Replacement checks.** `returns_corrected`: 12 rows, 0 failing records under the same rules,
ACCEPTED. It is the only source of the feature output.

**Independently derived expected value.** From the source row, `R01` has deduction 20 and income
100, so the expected ratio is 20 / 100 = 0.2. The notebook confirms those source values, reads
`R01` back from the saved `return_features` table and asserts it is 0.2 (it is).

**Saved-table readback (notebook outcome 3).** After the write, the notebook reads
`return_features` back and asserts: exactly the columns `return_id`, `tax_year`,
`deduction_to_income`; 12 rows, equal to the accepted batch; no repeated `return_id`; the saved
and accepted `return_id` sets are identical. Zero income: every zero-income source return (`R03`,
`R09`) is in the saved table with a NULL ratio.

## Two successful runs

Both runs are interactive Run all executions of the same notebook, each started in a clean
session, so no Jobs run IDs apply.

| Run | Executed (UTC) | Notebook URL | Snapshot |
| --- | --- | --- | --- |
| 1 | 2026-11-20 19:13:50 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558407 | `outputs/runs/run_20261120T191350Z.json` |
| 2 | 2026-11-20 19:16:48 | https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866558407 | `outputs/runs/run_20261120T191648Z.json` |

Each snapshot is written by the last cells of the notebook only after every check has passed,
and holds every row (`return_id`, `tax_year`, `deduction_to_income`) of `return_features` and the
three year tables. `outputs/run_comparison.json` compares the two snapshots row by row: for all
four tables the row counts are equal (12, 4, 4, 4), no `return_id` is in only one run and no value
differs, including the NULL ratios (`all_match: true`).

**Year assignment.** The split cell asserts each year table holds only its assigned `tax_year`
(2022, 2023, 2024).
**One copy of every accepted return.** The three year tables together have the same row count as
`return_features`, contain no repeated `return_id`, and match `return_features` exactly in both
directions (`exceptAll`). `return_features` itself matches the accepted batch's `return_id`s.
**No appended copies.** Every table and export is written with overwrite, and the second run
left the same row counts, IDs and values as the first.

## Final files and limitations

**Final exports under `outputs/`**

* `return_features.csv` (12 rows), `features_train.csv`, `features_validation.csv`,
  `features_test.csv` (4 rows each): exports of the saved tables, sorted by `return_id`; an empty
  ratio field is NULL.
* `batch_rejections.csv` (3 rows): affected initial records with rejection reasons.
* `runs/run_20261120T191350Z.json`, `runs/run_20261120T191648Z.json`: per-run snapshots.
* `run_comparison.json`: IDs-and-values comparison of the two runs.

Practice file, not part of the task: `connection-practice.json`.

**Not verified / limitations**

* The transformation from `parsed_source` to the two deliveries is not visible in either the
  lineage view or the Catalog Lineage tab. `returns_initial` has defects (a duplicate `R01`, a NULL
  `income` for `R02`) that `parsed_source` does not, so they were introduced by that hidden
  delivery step; its cause could not be inspected.
* The run comparison covers the four feature tables. `batch_rejections` is overwritten on each run
  and its export comes from the final run only.
* Both runs have the same notebook URL; the execution timestamps tell them apart.
* These checks establish this task's data contract only. They do not show that model leakage is
  ruled out; later work must add labels and assess what is known at prediction time.
* The final commit SHA is reported in Classroom, not in this file.
