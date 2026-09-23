# ioi — mirrored Snowflake DDL

A read-only mirror of the DPM pipeline as it actually exists in Snowflake,
laid out the same way as `data_pipeline_monitoring/snowflake/`:

```
ioi/
  <database lowercased>/
    <schema lowercased>.sql
```

Each file is the output of `GET_DDL('SCHEMA', '<DB>.<SCHEMA>', TRUE)` against
the live account, so it includes tables, sequences, streams, tasks and views —
the whole schema, not just the tables.

## Why this exists alongside `snowflake/`

`snowflake/` is the **intent**: hand-written, reviewed DDL that says what the
pipeline is supposed to be. `ioi/` is the **observation**: what the account
actually contains right now. They are expected to drift — the drift is the
interesting part, because it is exactly what a schema-drift check or an RCA
run is trying to explain.

Do not hand-edit these files. Regenerate them instead, or the mirror stops
being a mirror and becomes a second, unreviewed source of intent.

## ⚠️ Never run these files against a live account

`GET_DDL` emits `create or replace`, not `create if not exists`. Executing
`bronze.sql` against the real `DPM_SRC_CRM` would replace the table and
**destroy every landed row**. These files are for reading and diffing.
The reviewed, idempotent DDL in `data_pipeline_monitoring/snowflake/` is the
only thing meant to be deployed.

## Regenerating

The dump reads the encrypted Snowflake connector config out of the app's
Postgres database, so the backend must be configured (`backend/.env`) but does
not need to be running:

```bash
cd data_pipeline_monitoring/backend
PYTHONPATH=. uv run python ../../ioi/dump_ddl.py ../../ioi
```

## Coverage

| Database | Schemas mirrored |
|---|---|
| `DPM_SRC_CRM` | `BRONZE`, `SILVER` |
| `DPM_SRC_BILLING` | `BRONZE`, `SILVER` |
| `DPM_SRC_INVENTORY` | `BRONZE`, `SILVER` |
| `DPM_CUSTOMER_360` | `GOLD` |
| `DPM_INVENTORY_360` | `GOLD` |
| `DPM_SRC_LOYALTY` | `BRONZE`, `SILVER` — **not deployed yet** |
| `DPM_LOYALTY_360` | `GOLD` — **not deployed yet** |

The last two are in `dump_ddl.py`'s database list but have no files here yet.
They exist as reviewed intent in `data_pipeline_monitoring/snowflake/` and land
in this mirror the first time the dump runs after they are deployed; until then
the dump skips them with a `!!` line and refreshes the other five as usual.

They were added because the original five are all the same shape — an
append-only VARIANT landing table collapsed into entity state by a MERGE — and a
parity engine tested only against that shape is untested against most of what
real pipelines do. The loyalty pipeline is deliberately different: a CDC source
written MERGE-on-PK, a composite daily-snapshot grain, an SCD2 dimension, an
aggregate gold table, and a loader cursor table. See the header comment on
`snowflake/dpm_src_loyalty/bronze.sql` for what each one breaks.

Each database also has an empty `PUBLIC` schema in Snowflake. It holds no
objects and is not mirrored here.

`DPHM_TEST`, `SNOWFLAKE_LEARNING_DB` and `USER$ASHMIT` are visible to the
credentials but are out of scope — they are not part of the DPM pipeline.
