-- DPM_SRC_INVENTORY.SILVER
-- Mirrored from the live Snowflake account via
-- GET_DDL('SCHEMA', 'DPM_SRC_INVENTORY.SILVER', TRUE). Generated, not hand-written:
-- re-run the dump to refresh rather than editing this file.

create or replace schema DPM_SRC_INVENTORY.SILVER;

create or replace TABLE DPM_SRC_INVENTORY.SILVER.PRODUCTS (
	PRODUCT_ID NUMBER(38,0),
	PRODUCT_NAME VARCHAR(16777216),
	CATEGORY VARCHAR(16777216),
	UNIT_PRICE NUMBER(10,2),
	QUANTITY_ON_HAND NUMBER(38,0),
	WAREHOUSE_ID VARCHAR(16777216),
	UPDATED_AT TIMESTAMP_NTZ(9)
)COMMENT='Silver: cleaned/typed product inventory snapshot'
;
create or replace stream DPM_SRC_INVENTORY.SILVER.PRODUCTS_STREAM on table PRODUCTS;
