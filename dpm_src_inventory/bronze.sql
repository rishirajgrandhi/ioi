-- DPM_SRC_INVENTORY.BRONZE
-- Mirrored from the live Snowflake account via
-- GET_DDL('SCHEMA', 'DPM_SRC_INVENTORY.BRONZE', TRUE). Generated, not hand-written:
-- re-run the dump to refresh rather than editing this file.

create or replace schema DPM_SRC_INVENTORY.BRONZE;

create or replace sequence DPM_SRC_INVENTORY.BRONZE.SEQ_PRODUCT_ID start with 1 increment by 1 noorder;
create or replace TABLE DPM_SRC_INVENTORY.BRONZE.PRODUCTS_RAW (
	RECORD_ID NUMBER(38,0) autoincrement start 1 increment 1 noorder,
	RAW_PAYLOAD VARIANT,
	LOADED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
)COMMENT='Bronze: raw landed product/inventory snapshot records (append-only)'
;
create or replace stream DPM_SRC_INVENTORY.BRONZE.PRODUCTS_RAW_STREAM on table PRODUCTS_RAW append_only = true;
create or replace task DPM_SRC_INVENTORY.BRONZE.TASK_BRONZE_TO_SILVER_PRODUCTS
	warehouse=DPM_PIPELINE_WH
	schedule='1 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('DPM_SRC_INVENTORY.BRONZE.PRODUCTS_RAW_STREAM')
	as MERGE INTO DPM_SRC_INVENTORY.SILVER.PRODUCTS tgt
USING (
  SELECT
    RAW_PAYLOAD:product_id::NUMBER AS PRODUCT_ID,
    RAW_PAYLOAD:product_name::STRING AS PRODUCT_NAME,
    RAW_PAYLOAD:category::STRING AS CATEGORY,
    RAW_PAYLOAD:unit_price::NUMBER(10,2) AS UNIT_PRICE,
    RAW_PAYLOAD:quantity_on_hand::NUMBER AS QUANTITY_ON_HAND,
    -- BUG: the bronze payload key is "warehouse_id" (see
    -- SP_GENERATE_TEST_DATA in ../dpm_customer_360/generator.sql), but this
    -- reads "warehouse" instead, so WAREHOUSE_ID lands NULL for every row.
    RAW_PAYLOAD:warehouse::STRING AS WAREHOUSE_ID
  FROM DPM_SRC_INVENTORY.BRONZE.PRODUCTS_RAW_STREAM
  WHERE METADATA$ACTION = 'INSERT'
) src
ON tgt.PRODUCT_ID = src.PRODUCT_ID
WHEN MATCHED THEN UPDATE SET
  PRODUCT_NAME = src.PRODUCT_NAME, CATEGORY = src.CATEGORY, UNIT_PRICE = src.UNIT_PRICE,
  QUANTITY_ON_HAND = src.QUANTITY_ON_HAND, WAREHOUSE_ID = src.WAREHOUSE_ID, UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (PRODUCT_ID, PRODUCT_NAME, CATEGORY, UNIT_PRICE, QUANTITY_ON_HAND, WAREHOUSE_ID, UPDATED_AT)
  VALUES (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.UNIT_PRICE, src.QUANTITY_ON_HAND, src.WAREHOUSE_ID, CURRENT_TIMESTAMP());
