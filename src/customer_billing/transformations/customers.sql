-- Customer master data landed from the CRM export.

CREATE OR REFRESH STREAMING TABLE bronze.customers_raw
COMMENT 'Raw customer records landed from CRM'
AS SELECT *, current_timestamp() AS _ingest_ts
FROM STREAM read_files('${landing_root}/customers/', format => 'parquet', rescuedDataColumn => '_rescued_data');

CREATE OR REFRESH MATERIALIZED VIEW silver.dim_customer
COMMENT 'Current version of each customer'
AS SELECT
  customer_id, first_name, last_name, email, phone, street_address, city, postal_code, country_code,
  CASE
    WHEN country_code IN ('US', 'CA', 'MX') THEN 'NA'
    WHEN country_code IN ('GB', 'DE', 'FR', 'ES', 'IT', 'NL') THEN 'EMEA'
    WHEN country_code IN ('IN', 'JP', 'SG', 'AU') THEN 'APAC'
    ELSE 'OTHER'
  END AS region,
  lat, lon, tax_id, loyalty_tier, is_active, marketing_opt_in, profile_url, attributes_json, row_hash,
  created_at, updated_at
FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS _rn
  FROM bronze.customers_raw
)
WHERE _rn = 1;
