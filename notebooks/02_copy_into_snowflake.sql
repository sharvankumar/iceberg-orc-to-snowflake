-- =============================================================================
-- Load Iceberg ORC Data into Snowflake via COPY INTO
-- =============================================================================
-- Prerequisites:
--   1. Run 01_generate_iceberg_orc.ipynb to create ORC files on S3
--   2. An existing storage integration or external volume for S3 access
--   3. ACCOUNTADMIN or a role with CREATE STAGE, CREATE TABLE privileges
--
-- S3 source: s3://skumar-iceberg-lakehouse/iceberg/orc-demo/sales/
-- =============================================================================

-- ─────────────────────────────────────────────
-- 0. VARIABLES — UPDATE FOR YOUR ACCOUNT
-- ─────────────────────────────────────────────
SET WAREHOUSE          = 'COMPUTE_WH';
SET STORAGE_INTEGRATION = 'ICEBERG_S3_INT';  -- your existing S3 storage integration

-- ─────────────────────────────────────────────
-- 1. DATABASE & SCHEMA
-- ─────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($WAREHOUSE);

CREATE DATABASE IF NOT EXISTS ICEBERG_ORC_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS ICEBERG_ORC_DEMO_DB.SALES;
USE SCHEMA ICEBERG_ORC_DEMO_DB.SALES;

-- ─────────────────────────────────────────────
-- 2. FILE FORMAT
-- ─────────────────────────────────────────────
CREATE OR REPLACE FILE FORMAT orc_format
  TYPE = ORC;

-- ─────────────────────────────────────────────
-- 3. EXTERNAL STAGES (pointing to ORC data files on S3)
-- ─────────────────────────────────────────────
-- Each stage points to the data/ subdirectory where the .orc files live.
-- The Iceberg metadata/ directory is not needed for COPY INTO — we read
-- the ORC data files directly.

CREATE OR REPLACE STAGE customer_orders_orc_stage
  URL = 's3://skumar-iceberg-lakehouse/iceberg/orc-demo/sales/customer_orders/data/'
  STORAGE_INTEGRATION = IDENTIFIER($STORAGE_INTEGRATION)
  FILE_FORMAT = orc_format;

CREATE OR REPLACE STAGE product_catalog_orc_stage
  URL = 's3://skumar-iceberg-lakehouse/iceberg/orc-demo/sales/product_catalog/data/'
  STORAGE_INTEGRATION = IDENTIFIER($STORAGE_INTEGRATION)
  FILE_FORMAT = orc_format;

-- ─────────────────────────────────────────────
-- 4. LIST STAGED FILES (verify ORC files exist)
-- ─────────────────────────────────────────────
LIST @customer_orders_orc_stage;
LIST @product_catalog_orc_stage;

-- ─────────────────────────────────────────────
-- 5. CREATE TARGET TABLES
-- ─────────────────────────────────────────────
CREATE OR REPLACE TABLE ICEBERG_ORC_DEMO_DB.SALES.CUSTOMER_ORDERS (
    order_id     INT,
    customer_id  INT,
    product      VARCHAR,
    amount       NUMBER(10,2),
    order_date   DATE,
    region       VARCHAR
);

CREATE OR REPLACE TABLE ICEBERG_ORC_DEMO_DB.SALES.PRODUCT_CATALOG (
    product_id   INT,
    name         VARCHAR,
    category     VARCHAR,
    price        NUMBER(10,2)
);

-- ─────────────────────────────────────────────
-- 6. COPY INTO — Load ORC files into Snowflake
-- ─────────────────────────────────────────────
-- ORC is a self-describing format, so Snowflake can infer column mapping.
-- Use MATCH_BY_COLUMN_NAME to map ORC fields to table columns by name.

COPY INTO ICEBERG_ORC_DEMO_DB.SALES.CUSTOMER_ORDERS
  FROM @customer_orders_orc_stage
  FILE_FORMAT = (FORMAT_NAME = orc_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO ICEBERG_ORC_DEMO_DB.SALES.PRODUCT_CATALOG
  FROM @product_catalog_orc_stage
  FILE_FORMAT = (FORMAT_NAME = orc_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ─────────────────────────────────────────────
-- 7. VALIDATE — Verify data loaded correctly
-- ─────────────────────────────────────────────
SELECT 'CUSTOMER_ORDERS' AS table_name, COUNT(*) AS row_count
  FROM ICEBERG_ORC_DEMO_DB.SALES.CUSTOMER_ORDERS
UNION ALL
SELECT 'PRODUCT_CATALOG', COUNT(*)
  FROM ICEBERG_ORC_DEMO_DB.SALES.PRODUCT_CATALOG;

SELECT * FROM ICEBERG_ORC_DEMO_DB.SALES.CUSTOMER_ORDERS ORDER BY order_id;
SELECT * FROM ICEBERG_ORC_DEMO_DB.SALES.PRODUCT_CATALOG ORDER BY product_id;
