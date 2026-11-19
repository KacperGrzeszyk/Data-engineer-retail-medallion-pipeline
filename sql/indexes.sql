-- Optional indexes on foreign keys of the fact table (PostgreSQL).
-- The DAG recreates the Gold tables with if_exists='replace', so re-run this
-- script after each pipeline run (or add it as an extra Airflow task).
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales ("CustomerKey");
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON fact_sales ("ProductKey");
CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON fact_sales ("DateKey");
