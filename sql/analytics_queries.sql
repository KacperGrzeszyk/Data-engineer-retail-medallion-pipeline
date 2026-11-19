-- =====================================================================
-- Analytical queries on the Gold layer (star schema), PostgreSQL syntax
-- Tables: fact_sales, dim_customer, dim_product, dim_date
-- NOTE: pandas creates mixed-case columns, so they must be double-quoted.
-- NOTE: CustomerID = 0 is the "guest" customer (orders without an ID).
-- =====================================================================

-- 1. Monthly revenue trend with month-over-month growth -----------------
WITH monthly AS (
    SELECT d."Year", d."Month",
           SUM(f."Total_Amount")             AS revenue,
           COUNT(DISTINCT f."InvoiceNo")     AS orders
    FROM fact_sales f
    JOIN dim_date d ON d."DateKey" = f."DateKey"
    GROUP BY d."Year", d."Month"
)
SELECT "Year", "Month",
       ROUND(revenue::numeric, 2) AS revenue,
       orders,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY "Year", "Month"))
             / NULLIF(LAG(revenue) OVER (ORDER BY "Year", "Month"), 0), 1) AS mom_growth_pct
FROM monthly
ORDER BY "Year", "Month";

-- 2. Top 10 products by revenue ------------------------------------------
-- Non-product service codes (postage, manual adjustments, fees...) are excluded.
SELECT p."StockCode", p."Description",
       SUM(f."Quantity")                    AS units_sold,
       ROUND(SUM(f."Total_Amount")::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p."ProductKey" = f."ProductKey"
WHERE p."StockCode" NOT IN ('DOT', 'POST', 'M', 'B', 'C2', 'AMAZONFEE', 'BANK CHARGES')
GROUP BY p."StockCode", p."Description"
ORDER BY revenue DESC
LIMIT 10;

-- 3. Revenue by country (share of total) ---------------------------------
SELECT c."Country",
       ROUND(SUM(f."Total_Amount")::numeric, 2) AS revenue,
       ROUND(100.0 * SUM(f."Total_Amount") / SUM(SUM(f."Total_Amount")) OVER (), 1) AS share_pct
FROM fact_sales f
JOIN dim_customer c ON c."CustomerKey" = f."CustomerKey"
GROUP BY c."Country"
ORDER BY revenue DESC
LIMIT 10;

-- 4. Revenue by day of week ----------------------------------------------
SELECT d."DayOfWeek",
       COUNT(DISTINCT f."InvoiceNo")            AS orders,
       ROUND(SUM(f."Total_Amount")::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON d."DateKey" = f."DateKey"
GROUP BY d."DayOfWeek"
ORDER BY revenue DESC;

-- 5. Top 10 customers (excluding guest orders) ----------------------------
SELECT c."CustomerID",
       COUNT(DISTINCT f."InvoiceNo")            AS orders,
       ROUND(SUM(f."Total_Amount")::numeric, 2) AS revenue
FROM fact_sales f
JOIN dim_customer c ON c."CustomerKey" = f."CustomerKey"
WHERE c."CustomerID" <> 0
GROUP BY c."CustomerID"
ORDER BY revenue DESC
LIMIT 10;

-- 6. RFM segmentation (Recency, Frequency, Monetary) ----------------------
WITH ref AS (
    SELECT MAX("FullDate") + 1 AS ref_date FROM dim_date
),
customer_metrics AS (
    SELECT c."CustomerID",
           MAX(d."FullDate")                    AS last_purchase,
           COUNT(DISTINCT f."InvoiceNo")        AS frequency,
           SUM(f."Total_Amount")                AS monetary
    FROM fact_sales f
    JOIN dim_customer c ON c."CustomerKey" = f."CustomerKey"
    JOIN dim_date d     ON d."DateKey"     = f."DateKey"
    WHERE c."CustomerID" <> 0
    GROUP BY c."CustomerID"
),
scored AS (
    SELECT m."CustomerID",
           (SELECT ref_date FROM ref) - m.last_purchase AS recency_days,
           m.frequency,
           ROUND(m.monetary::numeric, 2)                AS monetary,
           NTILE(5) OVER (ORDER BY (SELECT ref_date FROM ref) - m.last_purchase DESC) AS r_score,
           NTILE(5) OVER (ORDER BY m.frequency)                                       AS f_score,
           NTILE(5) OVER (ORDER BY m.monetary)                                        AS m_score
    FROM customer_metrics m
)
SELECT "CustomerID", recency_days, frequency, monetary,
       r_score, f_score, m_score,
       CASE
           WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
           WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
           WHEN r_score >= 4 AND f_score <= 2 THEN 'New / Promising'
           WHEN r_score <= 2 AND f_score >= 3 THEN 'At risk'
           WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
           ELSE 'Needs attention'
       END AS segment
FROM scored
ORDER BY monetary DESC;

-- 7. Data quality checks on the Gold layer --------------------------------
-- 7a. Orphan foreign keys (every value should be 0)
SELECT
    SUM(CASE WHEN c."CustomerKey" IS NULL THEN 1 ELSE 0 END) AS orphan_customers,
    SUM(CASE WHEN p."ProductKey"  IS NULL THEN 1 ELSE 0 END) AS orphan_products,
    SUM(CASE WHEN d."DateKey"     IS NULL THEN 1 ELSE 0 END) AS orphan_dates
FROM fact_sales f
LEFT JOIN dim_customer c ON c."CustomerKey" = f."CustomerKey"
LEFT JOIN dim_product  p ON p."ProductKey"  = f."ProductKey"
LEFT JOIN dim_date     d ON d."DateKey"     = f."DateKey";

-- 7b. Non-positive prices or quantities (should be 0)
SELECT COUNT(*) AS bad_rows
FROM fact_sales
WHERE "Quantity" <= 0 OR "UnitPrice" <= 0;
