CREATE EXTERNAL TABLE sales_analytics (
order_id string,
order_date string,
customer_id string,
product_name string,
product_category string,
quantity int,
sales double,
first_name string,
last_name string,
country string,
state string,
city string,
score int,
customer_name string,
revenue double
)
STORED AS PARQUET
LOCATION 's3://de-csv-final-data/sales-analytics/';

SELECT
state,
SUM(revenue) AS total_sales
FROM sales_analytics
GROUP BY state
ORDER BY total_sales DESC;

select * from sales_analytics;

-- Check row count and no nulls
SELECT
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT order_id)        AS unique_orders,
    COUNT(DISTINCT customer_id)     AS unique_customers,
    SUM(CASE WHEN score IS NULL THEN 1 ELSE 0 END) AS null_scores,
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS null_revenue,
    ROUND(SUM(revenue), 2)          AS total_revenue
FROM "my-sales-data-catalog-db"."clean_sales_analytics_processed";

-- Preview the new derived columns
SELECT order_id, order_date, order_year, order_month,
       customer_name, customer_tier, product_category,
       revenue, revenue_per_unit
FROM "my-sales-data-catalog-db"."clean_sales_analytics_processed"
LIMIT 10;