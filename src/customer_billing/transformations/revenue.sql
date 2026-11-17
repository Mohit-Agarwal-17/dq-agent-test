-- Paid revenue per month. Two temporary views feed the published table; temporary views are pipeline-internal
-- and are not published to the catalog.
CREATE TEMPORARY VIEW paid_invoices AS
SELECT invoice_id, customer_id, invoice_date, total
FROM silver.invoices
WHERE payment_status = 'PAID';

CREATE TEMPORARY VIEW paid_invoices_by_month AS
SELECT date_trunc('MONTH', invoice_date) AS revenue_month, customer_id, total
FROM paid_invoices;

CREATE OR REFRESH MATERIALIZED VIEW gold.monthly_revenue
COMMENT 'Paid revenue per month'
AS SELECT revenue_month, SUM(total) AS revenue, COUNT(DISTINCT customer_id) AS paying_customers
FROM paid_invoices_by_month
GROUP BY revenue_month;
