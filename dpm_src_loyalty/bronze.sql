-- DPM_SRC_LOYALTY.BRONZE
-- Mirrored from the live Snowflake account via
-- GET_DDL('SCHEMA', 'DPM_SRC_LOYALTY.BRONZE', TRUE). Generated, not hand-written:
-- re-run the dump to refresh rather than editing this file.

create or replace schema DPM_SRC_LOYALTY.BRONZE;

create or replace sequence DPM_SRC_LOYALTY.BRONZE.SEQ_MEMBER_ID start with 1 increment by 1 noorder;
create or replace TABLE DPM_SRC_LOYALTY.BRONZE.CURSOR_STATE (
	TABLE_NAME VARCHAR(16777216) NOT NULL,
	LAST_CURSOR VARCHAR(16777216),
	PAGES_LOADED NUMBER(38,0),
	ROWS_LOADED NUMBER(38,0),
	STATUS VARCHAR(16777216),
	UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='Bronze: per-table loader watermark and row accounting'
;
create or replace TABLE DPM_SRC_LOYALTY.BRONZE.MEMBERS_RAW (
	MEMBER_ID NUMBER(38,0) NOT NULL,
	OP VARCHAR(16777216) NOT NULL,
	SEQ_NO NUMBER(38,0) NOT NULL,
	RAW_PAYLOAD VARIANT,
	ETL_LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ETL_UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='Bronze: loyalty members, written MERGE-on-PK (one row per entity, latest CDC event)'
;
create or replace TABLE DPM_SRC_LOYALTY.BRONZE.POINTS_SNAPSHOT_RAW (
	RECORD_ID NUMBER(38,0) autoincrement start 1 increment 1 noorder,
	MEMBER_ID NUMBER(38,0) NOT NULL,
	SNAPSHOT_DATE DATE NOT NULL,
	RAW_PAYLOAD VARIANT,
	LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='Bronze: daily loyalty point snapshots, append-only, grain (MEMBER_ID, SNAPSHOT_DATE)'
;
create or replace stream DPM_SRC_LOYALTY.BRONZE.MEMBERS_RAW_STREAM on table MEMBERS_RAW;
create or replace stream DPM_SRC_LOYALTY.BRONZE.POINTS_SNAPSHOT_RAW_STREAM on table POINTS_SNAPSHOT_RAW append_only = true;
create or replace task DPM_SRC_LOYALTY.BRONZE.TASK_BRONZE_TO_SILVER_MEMBERS_CLOSE
	warehouse=DPM_PIPELINE_WH
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('DPM_SRC_LOYALTY.BRONZE.MEMBERS_RAW_STREAM')
	as MERGE INTO DPM_SRC_LOYALTY.SILVER.MEMBERS tgt
USING (
  SELECT
    s.MEMBER_ID AS MEMBER_ID,
    s.OP AS OP,
    s.RAW_PAYLOAD:tier::STRING AS TIER,
    s.RAW_PAYLOAD:status::STRING AS STATUS,
    -- ETL_UPDATED_AT, not ETL_LOADED_AT. On a MERGE-on-PK table only the
    -- update timestamp moves when a change arrives; the load timestamp keeps
    -- the value it got when the entity was first inserted. Closing a version
    -- at its own VALID_FROM makes a zero-length window, so every member that
    -- ever changed would report as a malformed window and a duplicate - the
    -- pipeline behaving exactly as designed, with the check failing forever.
    s.ETL_UPDATED_AT AS CHANGED_AT
  FROM DPM_SRC_LOYALTY.BRONZE.MEMBERS_RAW_STREAM s
  WHERE s.MEMBER_ID IS NOT NULL
  -- One row per member per batch: the newest change wins. PARTITION BY names
  -- the natural key and ORDER BY names the sequence, which together are the
  -- whole dedup contract.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.MEMBER_ID
    ORDER BY s.SEQ_NO DESC
  ) = 1
) src
  ON tgt.MEMBER_ID IS NOT DISTINCT FROM src.MEMBER_ID
 AND tgt.IS_CURRENT = TRUE
WHEN MATCHED
 AND (
      -- A tombstone closes the member outright: it is gone from the source, so
      -- it must stop being current here. Without this arm a deleted member
      -- keeps a live row in silver forever, and the parity check - which
      -- filters the source to OP <> 'D' - reports it as a key silver invented.
      src.OP = 'D'
      OR tgt.TIER IS DISTINCT FROM src.TIER
      OR tgt.STATUS IS DISTINCT FROM src.STATUS
     )
THEN UPDATE SET
  VALID_TO = src.CHANGED_AT,
  IS_CURRENT = FALSE,
  UPDATED_AT = CURRENT_TIMESTAMP();
create or replace task DPM_SRC_LOYALTY.BRONZE.TASK_BRONZE_TO_SILVER_MEMBERS_OPEN
	warehouse=DPM_PIPELINE_WH
	after DPM_SRC_LOYALTY.BRONZE.TASK_BRONZE_TO_SILVER_MEMBERS_CLOSE
	as MERGE INTO DPM_SRC_LOYALTY.SILVER.MEMBERS tgt
USING (
  SELECT
    b.MEMBER_ID AS MEMBER_ID,
    b.RAW_PAYLOAD:full_name::STRING AS FULL_NAME,
    b.RAW_PAYLOAD:email::STRING AS EMAIL,
    b.RAW_PAYLOAD:tier::STRING AS TIER,
    b.RAW_PAYLOAD:status::STRING AS STATUS,
    b.RAW_PAYLOAD:enrolled_date::DATE AS ENROLLED_DATE,
    b.ETL_UPDATED_AT AS CHANGED_AT
  FROM DPM_SRC_LOYALTY.BRONZE.MEMBERS_RAW b
  -- The lifted filter. A tombstone is a real bronze row; it is simply not a
  -- member silver should carry as current.
  WHERE b.OP <> 'D'
    AND b.MEMBER_ID IS NOT NULL
) src
  ON tgt.MEMBER_ID IS NOT DISTINCT FROM src.MEMBER_ID
 AND tgt.IS_CURRENT = TRUE
WHEN NOT MATCHED THEN INSERT (
  MEMBER_ID, FULL_NAME, EMAIL, TIER, STATUS, ENROLLED_DATE,
  VALID_FROM, VALID_TO, IS_CURRENT, UPDATED_AT
) VALUES (
  src.MEMBER_ID, src.FULL_NAME, src.EMAIL, src.TIER, src.STATUS, src.ENROLLED_DATE,
  src.CHANGED_AT, '9999-12-31'::TIMESTAMP_NTZ, TRUE, CURRENT_TIMESTAMP()
);
create or replace task DPM_SRC_LOYALTY.BRONZE.TASK_BRONZE_TO_SILVER_POINTS_DAILY
	warehouse=DPM_PIPELINE_WH
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('DPM_SRC_LOYALTY.BRONZE.POINTS_SNAPSHOT_RAW_STREAM')
	as MERGE INTO DPM_SRC_LOYALTY.SILVER.POINTS_DAILY tgt
USING (
  SELECT
    s.MEMBER_ID AS MEMBER_ID,
    s.SNAPSHOT_DATE AS SNAPSHOT_DATE,
    s.RAW_PAYLOAD:points_earned::NUMBER AS POINTS_EARNED,
    s.RAW_PAYLOAD:points_redeemed::NUMBER AS POINTS_REDEEMED,
    s.RAW_PAYLOAD:channel::STRING AS CHANNEL
  FROM DPM_SRC_LOYALTY.BRONZE.POINTS_SNAPSHOT_RAW_STREAM s
  WHERE METADATA$ACTION = 'INSERT'
    AND s.MEMBER_ID IS NOT NULL
    AND s.SNAPSHOT_DATE IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.MEMBER_ID, s.SNAPSHOT_DATE
    ORDER BY s.RECORD_ID DESC
  ) = 1
) src
  -- Both key columns. Keying on MEMBER_ID alone would collapse the series to
  -- one row per member, which is the bug this table's grain exists to avoid.
  ON tgt.MEMBER_ID = src.MEMBER_ID
 AND tgt.SNAPSHOT_DATE = src.SNAPSHOT_DATE
WHEN NOT MATCHED THEN INSERT (
  MEMBER_ID, SNAPSHOT_DATE, POINTS_EARNED, POINTS_REDEEMED, CHANNEL, UPDATED_AT
) VALUES (
  src.MEMBER_ID, src.SNAPSHOT_DATE, src.POINTS_EARNED, src.POINTS_REDEEMED, src.CHANNEL, CURRENT_TIMESTAMP()
);
