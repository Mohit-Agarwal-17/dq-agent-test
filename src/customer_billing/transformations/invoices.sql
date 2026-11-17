-- Invoice change feed from the billing system, applied as SCD type 1.

CREATE OR REFRESH STREAMING TABLE bronze.invoices_raw
COMMENT 'Raw invoice change feed from billing'
AS SELECT * FROM STREAM read_files('${landing_root}/invoices/', format => 'parquet', rescuedDataColumn => '_rescued_data');

CREATE OR REFRESH STREAMING TABLE silver.invoices
COMMENT 'Current version of each invoice';

CREATE FLOW invoices_cdc AS AUTO CDC INTO silver.invoices
FROM STREAM(bronze.invoices_raw)
KEYS (invoice_id)
SEQUENCE BY version_seq
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE silver.shipments
COMMENT 'Shipment events per order'
AS SELECT
  shipment_id, order_id, carrier_code, tracking_url, shipped_at, delivered_at,
  weight_kg, package_count, delivery_status, signature_required
FROM STREAM read_files('${landing_root}/shipments/', format => 'parquet');

CREATE OR REFRESH MATERIALIZED VIEW gold.customer_ltv
COMMENT 'Lifetime billed value per customer'
AS SELECT
  i.customer_id,
  SUM(i.total) AS lifetime_value,
  COUNT(*) AS invoice_count,
  MAX(i.invoice_date) AS last_invoice_date
FROM silver.invoices i
LEFT JOIN silver.dim_customer c ON i.customer_id = c.customer_id
GROUP BY i.customer_id;
