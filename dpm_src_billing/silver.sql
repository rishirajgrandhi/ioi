-- DPM_SRC_BILLING.SILVER
-- Mirrored from the live Snowflake account via
-- GET_DDL('SCHEMA', 'DPM_SRC_BILLING.SILVER', TRUE). Generated, not hand-written:
-- re-run the dump to refresh rather than editing this file.

create or replace schema DPM_SRC_BILLING.SILVER;

create or replace TABLE DPM_SRC_BILLING.SILVER.INVOICES (
	INVOICE_ID NUMBER(38,0),
	CUSTOMER_ID NUMBER(38,0),
	AMOUNT NUMBER(12,2),
	STATUS VARCHAR(16777216),
	INVOICE_DATE DATE,
	UPDATED_AT TIMESTAMP_NTZ(9)
)COMMENT='Silver: cleaned/typed billing invoices'
;
create or replace stream DPM_SRC_BILLING.SILVER.INVOICES_STREAM on table INVOICES;
