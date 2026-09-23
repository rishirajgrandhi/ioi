-- DPM_SRC_CRM.SILVER
-- Mirrored from the live Snowflake account via
-- GET_DDL('SCHEMA', 'DPM_SRC_CRM.SILVER', TRUE). Generated, not hand-written:
-- re-run the dump to refresh rather than editing this file.

create or replace schema DPM_SRC_CRM.SILVER;

create or replace TABLE DPM_SRC_CRM.SILVER.CUSTOMERS (
	CUSTOMER_ID NUMBER(38,0),
	FULL_NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	SIGNUP_DATE DATE,
	UPDATED_AT TIMESTAMP_NTZ(9)
)COMMENT='Silver: cleaned/typed CRM customers'
;
create or replace stream DPM_SRC_CRM.SILVER.CUSTOMERS_STREAM on table CUSTOMERS;
