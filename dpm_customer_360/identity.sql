-- DPM_CUSTOMER_360.IDENTITY
-- Mirrored from the live Snowflake account via
-- GET_DDL('SCHEMA', 'DPM_CUSTOMER_360.IDENTITY', TRUE). Generated, not hand-written:
-- re-run the dump to refresh rather than editing this file.

create or replace schema DPM_CUSTOMER_360.IDENTITY;

create or replace TABLE DPM_CUSTOMER_360.IDENTITY.INDIVIDUAL_XREF (
	SOURCE_SYSTEM VARCHAR(16777216) NOT NULL,
	SOURCE_ID VARCHAR(16777216) NOT NULL,
	INDIVIDUAL_ID VARCHAR(16777216) NOT NULL,
	MATCH_RULE VARCHAR(16777216) NOT NULL,
	RESOLVED_AT TIMESTAMP_NTZ(9)
)COMMENT='Identity: (source system, source id) -> individual. Must be a function, not a relation'
;
create or replace TABLE DPM_CUSTOMER_360.IDENTITY.NORMALIZE_EMAIL (
	SOURCE_SYSTEM VARCHAR(16777216) NOT NULL,
	SOURCE_ID VARCHAR(16777216) NOT NULL,
	RAW_EMAIL VARCHAR(16777216),
	NORMALIZED_EMAIL VARCHAR(16777216),
	UPDATED_AT TIMESTAMP_NTZ(9)
)COMMENT='Identity: one row per source record, with its email normalised for matching'
;
CREATE OR REPLACE PROCEDURE DPM_CUSTOMER_360.IDENTITY.SP_RESOLVE_IDENTITY()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT='Rebuilds NORMALIZE_EMAIL and INDIVIDUAL_XREF from the CRM and loyalty silver layers'
EXECUTE AS OWNER
AS '
DECLARE
  normalized INTEGER DEFAULT 0;
  resolved INTEGER DEFAULT 0;
  individuals INTEGER DEFAULT 0;
BEGIN
  -- Rebuilt in place. TRUNCATE + INSERT rather than CREATE OR REPLACE keeps
  -- the table''s identity, its grants and any stream on it intact.
  TRUNCATE TABLE DPM_CUSTOMER_360.IDENTITY.NORMALIZE_EMAIL;

  INSERT INTO DPM_CUSTOMER_360.IDENTITY.NORMALIZE_EMAIL
    (SOURCE_SYSTEM, SOURCE_ID, RAW_EMAIL, NORMALIZED_EMAIL, UPDATED_AT)
  SELECT
      ''CRM'',
      TO_VARCHAR(c.CUSTOMER_ID),
      c.EMAIL,
      NULLIF(REGEXP_REPLACE(LOWER(TRIM(c.EMAIL)), ''[+][^@]*@'', ''@''), ''''),
      CURRENT_TIMESTAMP()
  FROM DPM_SRC_CRM.SILVER.CUSTOMERS c
  UNION ALL
  SELECT
      ''LOYALTY'',
      TO_VARCHAR(m.MEMBER_ID),
      m.EMAIL,
      NULLIF(REGEXP_REPLACE(LOWER(TRIM(m.EMAIL)), ''[+][^@]*@'', ''@''), ''''),
      CURRENT_TIMESTAMP()
  FROM DPM_SRC_LOYALTY.SILVER.MEMBERS m
  -- Current versions only. The dimension keeps history; identity is about who
  -- the person is now, and matching on superseded emails would resurrect
  -- addresses the person has already replaced.
  WHERE m.IS_CURRENT = TRUE;

  normalized := SQLROWCOUNT;

  TRUNCATE TABLE DPM_CUSTOMER_360.IDENTITY.INDIVIDUAL_XREF;

  INSERT INTO DPM_CUSTOMER_360.IDENTITY.INDIVIDUAL_XREF
    (SOURCE_SYSTEM, SOURCE_ID, INDIVIDUAL_ID, MATCH_RULE, RESOLVED_AT)
  SELECT
      n.SOURCE_SYSTEM,
      n.SOURCE_ID,
      -- Records sharing a normalised email are one person. Records without one
      -- cannot match anybody, so each becomes an individual of its own keyed on
      -- its source identity - dropping them instead would make the customer
      -- disappear from the 360 with nothing to show it had ever been there.
      CASE
        WHEN n.NORMALIZED_EMAIL IS NOT NULL THEN MD5(n.NORMALIZED_EMAIL)
        ELSE MD5(n.SOURCE_SYSTEM || '':'' || n.SOURCE_ID)
      END,
      CASE
        WHEN n.NORMALIZED_EMAIL IS NOT NULL THEN ''EMAIL_EXACT''
        ELSE ''UNMATCHABLE_SINGLETON''
      END,
      CURRENT_TIMESTAMP()
  FROM DPM_CUSTOMER_360.IDENTITY.NORMALIZE_EMAIL n
  -- One row per source record. A source that lands the same id twice would
  -- otherwise make the cross-reference a relation rather than a function, and
  -- every downstream count would double for that person only.
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY n.SOURCE_SYSTEM, n.SOURCE_ID
    ORDER BY n.UPDATED_AT DESC
  ) = 1;

  resolved := SQLROWCOUNT;

  SELECT COUNT(DISTINCT INDIVIDUAL_ID) INTO :individuals
  FROM DPM_CUSTOMER_360.IDENTITY.INDIVIDUAL_XREF;

  RETURN ''Normalised '' || normalized || '' source record(s); resolved '' || resolved
      || '' of them onto '' || individuals || '' individual(s).'';
END;
';
create or replace task DPM_CUSTOMER_360.IDENTITY.TASK_RESOLVE_IDENTITY
	warehouse=DPM_PIPELINE_WH
	schedule='5 MINUTE'
	COMMENT='Rebuilds the identity cross-reference that the gold layer keys on'
	as CALL DPM_CUSTOMER_360.IDENTITY.SP_RESOLVE_IDENTITY();
