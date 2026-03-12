-- =============================================================================
-- Load Iceberg ORC Data into Snowflake via COPY INTO
-- =============================================================================
-- Prerequisites:
--   1. Run 01_generate_iceberg_orc.ipynb to create ORC files on S3
--   2. An existing storage integration or external volume for S3 access
--   3. ACCOUNTADMIN or a role with CREATE STAGE, CREATE TABLE privileges
--
-- S3 source: s3://skumar-iceberg-lakehouse/iceberg/orc-demo-v2/sales/
-- =============================================================================

-- ─────────────────────────────────────────────
-- 0. VARIABLES — UPDATE FOR YOUR ACCOUNT
-- ─────────────────────────────────────────────
SET EXT_VOLUME          = 'iceberg_external_volume';          -- your S3 external volume name
SET WAREHOUSE           = 'COMPUTE_WH';
SET STORAGE_INTEGRATION = 'S3_SKUMAR_ICEBERG_LAKEHOUSE';      -- your existing S3 storage integration

-- ─────────────────────────────────────────────
-- 1. DATABASE & SCHEMA
-- ─────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE IDENTIFIER($WAREHOUSE);

CREATE DATABASE IF NOT EXISTS ICEBERG_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS ICEBERG_DEMO_DB.SALES;
USE SCHEMA ICEBERG_DEMO_DB.SALES;

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
  URL = 's3://skumar-iceberg-lakehouse/iceberg/orc-demo-v2/sales/customer_orders/data/'
  STORAGE_INTEGRATION = S3_SKUMAR_ICEBERG_LAKEHOUSE
  FILE_FORMAT = orc_format;

CREATE OR REPLACE STAGE product_catalog_orc_stage
  URL = 's3://skumar-iceberg-lakehouse/iceberg/orc-demo-v2/sales/product_catalog/data/'
  STORAGE_INTEGRATION = S3_SKUMAR_ICEBERG_LAKEHOUSE
  FILE_FORMAT = orc_format;

-- ─────────────────────────────────────────────
-- 4. LIST STAGED FILES (verify ORC files exist)
-- ─────────────────────────────────────────────
LIST @customer_orders_orc_stage;
LIST @product_catalog_orc_stage;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║  PART A: REGULAR SNOWFLAKE TABLES  (standard COPY INTO)                   ║
-- ║  Commented out — uncomment if you also want regular (non-Iceberg) tables  ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝

-- -- 5a. CREATE REGULAR TARGET TABLES
-- CREATE OR REPLACE TABLE ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS (
--     order_id     INT,
--     customer_id  INT,
--     product      VARCHAR,
--     amount       NUMBER(10,2),
--     order_date   DATE,
--     region       VARCHAR
-- );
--
-- CREATE OR REPLACE TABLE ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG (
--     product_id   INT,
--     name         VARCHAR,
--     category     VARCHAR,
--     price        NUMBER(10,2)
-- );
--
-- -- 6a. COPY INTO — Load ORC files into regular tables
-- COPY INTO ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS
--   FROM @customer_orders_orc_stage
--   FILE_FORMAT = (FORMAT_NAME = orc_format)
--   MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
--
-- COPY INTO ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG
--   FROM @product_catalog_orc_stage
--   FILE_FORMAT = (FORMAT_NAME = orc_format)
--   MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
--
-- -- 7a. VALIDATE REGULAR TABLES
-- SELECT 'ORC_CUSTOMER_ORDERS' AS table_name, COUNT(*) AS row_count
--   FROM ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS
-- UNION ALL
-- SELECT 'ORC_PRODUCT_CATALOG', COUNT(*)
--   FROM ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG;
--
-- SELECT * FROM ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS ORDER BY order_id;
-- SELECT * FROM ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG ORDER BY product_id;


-- ╔═════════════════════════════════════════════════════════════════════════════╗
-- ║  PART B: SNOWFLAKE-MANAGED ICEBERG TABLES                                 ║
-- ╚═════════════════════════════════════════════════════════════════════════════╝
-- These tables are stored in Iceberg format on S3 via the external volume,
-- with Snowflake acting as the Iceberg catalog (CATALOG = 'SNOWFLAKE').
-- Snowflake manages the Iceberg metadata and writes Parquet data files
-- to the BASE_LOCATION under the external volume.

-- ─────────────────────────────────────────────
-- 5b. CREATE ICEBERG TARGET TABLES
-- ─────────────────────────────────────────────
CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS (
    order_id     INT,
    customer_id  INT,
    product      VARCHAR,
    amount       NUMBER(10,2),
    order_date   DATE,
    region       VARCHAR
)
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  BASE_LOCATION   = 'iceberg_demo/sales/orc_customer_orders/';

CREATE OR REPLACE ICEBERG TABLE ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG (
    product_id   INT,
    name         VARCHAR,
    category     VARCHAR,
    price        NUMBER(10,2)
)
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  BASE_LOCATION   = 'iceberg_demo/sales/orc_product_catalog/';

-- ─────────────────────────────────────────────
-- 6b. COPY INTO — Load ORC files directly into Iceberg tables
-- ─────────────────────────────────────────────
COPY INTO ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS
  FROM @customer_orders_orc_stage
  FILE_FORMAT = (FORMAT_NAME = orc_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG
  FROM @product_catalog_orc_stage
  FILE_FORMAT = (FORMAT_NAME = orc_format)
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ─────────────────────────────────────────────
-- 7b. VALIDATE ICEBERG TABLES
-- ─────────────────────────────────────────────
SELECT 'ORC_CUSTOMER_ORDERS' AS table_name, COUNT(*) AS row_count
  FROM ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS
UNION ALL
SELECT 'ORC_PRODUCT_CATALOG', COUNT(*)
  FROM ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG;

SELECT * FROM ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS ORDER BY order_id;
SELECT * FROM ICEBERG_DEMO_DB.SALES.ORC_PRODUCT_CATALOG ORDER BY product_id;

-- ─────────────────────────────────────────────
-- 8. INSPECT ICEBERG METADATA
-- ─────────────────────────────────────────────
-- Confirm these are Iceberg tables and see where Snowflake writes their files.
SHOW ICEBERG TABLES IN SCHEMA ICEBERG_DEMO_DB.SALES;

DESCRIBE TABLE ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS;
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_DEMO_DB.SALES.ORC_CUSTOMER_ORDERS')
  AS iceberg_metadata;
