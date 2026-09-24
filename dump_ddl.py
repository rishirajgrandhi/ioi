"""Mirror live Snowflake DDL for the DPM_* databases into ioi/.

Read-only: SHOW SCHEMAS + GET_DDL only. Writes nothing to Snowflake.
"""
import json
import pathlib
import sys

from app.connectors.registry import build_connector
from app.db import SessionLocal
from app.models import Connector

OUT = pathlib.Path(sys.argv[1])
DATABASES = [
    "DPM_SRC_CRM",
    "DPM_SRC_BILLING",
    "DPM_SRC_INVENTORY",
    "DPM_SRC_LOYALTY",
    # One 360, not three. The sources are many and the data product is one -
    # the same shape FCC's Fan360 has, and the reason the other two gold
    # databases were retired from the design.
    "DPM_CUSTOMER_360",
]

db = SessionLocal()
row = db.query(Connector).filter(Connector.type == "SNOWFLAKE").first()
if row is None:
    sys.exit("no SNOWFLAKE connector row found")
conn = build_connector(row.type, dict(row.config))

manifest = {}
try:
    ident = conn.run_query(
        "SELECT CURRENT_ACCOUNT() A, CURRENT_USER() U, CURRENT_ROLE() R, CURRENT_WAREHOUSE() W"
    )[0]
    print("identity:", json.dumps(ident), flush=True)

    for database in DATABASES:
        try:
            schemas = conn.run_query(f"SHOW SCHEMAS IN DATABASE {database}")
        except Exception as exc:
            print(f"!! {database}: {exc}", flush=True)
            continue
        names = [
            str(s["name"])
            for s in schemas
            if str(s["name"]).upper() != "INFORMATION_SCHEMA"
        ]
        manifest[database] = names
        for schema in names:
            ddl = conn.run_query(
                f"SELECT GET_DDL('SCHEMA', '{database}.{schema}', TRUE) AS DDL"
            )[0]["DDL"]
            path = OUT / database.lower() / f"{schema.lower()}.sql"
            path.parent.mkdir(parents=True, exist_ok=True)
            header = (
                f"-- {database}.{schema}\n"
                f"-- Mirrored from the live Snowflake account via\n"
                f"-- GET_DDL('SCHEMA', '{database}.{schema}', TRUE). Generated, not hand-written:\n"
                f"-- re-run the dump to refresh rather than editing this file.\n\n"
            )
            path.write_text(header + ddl.strip() + "\n", encoding="utf-8")
            print(f"wrote {path}  ({len(ddl)} chars)", flush=True)
finally:
    conn.close()
    db.close()

print("MANIFEST " + json.dumps(manifest))
