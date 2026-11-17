# lakeflow-pipelines (sample)

Sample Declarative Automation Bundle used by the DQ agent demo.

| Pipeline | Language | Datasets |
|---|---|---|
| sales_orders | SDP Python | bronze.orders_raw, silver.orders, gold.orders_daily |
| customer_billing | SDP SQL | bronze.customers_raw, silver.dim_customer, bronze.invoices_raw, silver.invoices (AUTO CDC), silver.shipments, gold.customer_ltv |
