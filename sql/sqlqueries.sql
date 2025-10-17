/*
Problem:
A retail company wants to understand its customers, products, and revenue trends to make data-driven decisions. They face challenges in:
Identifying top customers and high-value segments.
Understanding product performance and repeat purchase behavior.
Tracking customer retention and churn risk.
Monitoring revenue trends and operational KPIs.
*/
--create adatbase named retail_analytics
create database retail_analytics

--use the datbase
select current_database()

--drops the table if already exists
DROP TABLE IF EXISTS reviews CASCADE;
/*Insert: Cannot insert into child table if it is not inserted in parent table
delete : Cnnot delete the parent table id, if it is not deleted from child table first 
but cascade overcomes this*/

-- tables schema creation is just shown here for reference.the actal table structure is created in etl script
-- create customers table
CREATE TABLE customers (
  customer_id VARCHAR PRIMARY KEY,
  name TEXT,
  email TEXT,
  signup_date DATE,
  city TEXT,
  state TEXT,
  country TEXT,
  dob DATE,
  gender VARCHAR(10)
);

/*
| Type                  | Precision         | Storage  | Use case                                               |
| --------------------- | ----------------- | -------- | ------------------------------------------------------ |
| `numeric` / `decimal` | Exact (specified) | Variable | Money, accounting, exact values                        |
| `double precision`    | Approximate       | 8 bytes  | Scientific, measurements, calculations with huge range |
numeric(10,2) → exactly 2 decimal places.
double precision → many decimals, but approximate.
numeric can store integers, decimals, and very large numbers exactly. */
--create products table
CREATE TABLE products (
  product_id VARCHAR PRIMARY KEY,
  product_name TEXT,
  category TEXT,
  price NUMERIC,
  cost NUMERIC
);

--create orders table
CREATE TABLE orders (
  order_id VARCHAR PRIMARY KEY,
  order_date DATE,
  customer_id VARCHAR REFERENCES customers(customer_id),
  order_amount NUMERIC,
  payment_method TEXT,
  order_status TEXT
);

--create order_items table
CREATE TABLE order_items (
  order_item_id VARCHAR PRIMARY KEY,
  order_id VARCHAR REFERENCES orders(order_id),
  product_id VARCHAR REFERENCES products(product_id),
  quantity INT,
  unit_price NUMERIC
);

--create reviews table
CREATE TABLE reviews (
  review_id VARCHAR PRIMARY KEY,
  order_id VARCHAR REFERENCES orders(order_id),
  customer_id VARCHAR REFERENCES customers(customer_id),
  rating INT,
  review_text TEXT,
  review_date DATE
);

select * from customers; where signup_date is not null;
select * from products;
select * from orders;
select * from order_items;
select * from reviews;

SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'orders';

-- little bit cleaning after etl pipeline
SELECT *
FROM customers
WHERE name IS NULL
  AND email IS NOT NULL;


UPDATE customers
SET name = SPLIT_PART(email, '@', 1)
WHERE name IS NULL 
  AND email IS NOT NULL;

select * from customers where name is null; -- cleaned

-- tranformation
alter table orders alter column order_date type DATE;


-- Exploratory Data analysis
-- Customer Lifetime Value (CLV) Distribution
/* What is the distribution of total revenue per customer */
SELECT 
    c.customer_id,
    ROUND(SUM(oi.unit_price * oi.quantity)::numeric, 2) AS lifetime_value
	/* ROUND(numeric) → rounds to nearest integer
ROUND(numeric, integer) → rounds to integer decimal places only if the first argument 
is type numeric, not double precision.
SUM(oi.unit_price * oi.quantity) or sum(order_numner) is  double precision, so ROUND(..., 2) fails 
cast to numeric first */
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id
ORDER BY lifetime_value DESC;


-- Repeat Purchase Behavior
/* What percentage of customers purchase more than 3 times?*/
-- Repeat Purchase Behavior
WITH customer_orders AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders
    FROM orders
    GROUP BY customer_id
)
SELECT 
    ROUND(100.0 * COUNT(*) FILTER (WHERE total_orders > 3) / COUNT(*), 2) AS pct_repeat_buyers
FROM customer_orders;

-- Time between purchase
/* What is the average time gap between consecutive purchases per customer? */
-- Time Between Purchases
WITH purchase_gaps AS (
    SELECT 
        customer_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
    FROM orders
)
SELECT 
    customer_id,
    ROUND(AVG(order_date - prev_order_date), 2) AS avg_days_between_orders
FROM purchase_gaps
WHERE prev_order_date IS NOT NULL
GROUP BY customer_id
ORDER BY avg_days_between_orders;


-- Churn Risk Analysis
/* How many customers haven’t purchased in the last 3 months despite being previously active? */
WITH last_purchase AS (
    SELECT 
        customer_id,
        MAX(order_date) AS last_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT 
    COUNT(*) AS churned_customers
FROM last_purchase
WHERE last_order_date < (CURRENT_DATE - INTERVAL '3 months');


-- Revenue Trend Analysis
/* How is revenue trending month over month */
-- Revenue Trend Analysis
SELECT 
    DATE_TRUNC('month', o.order_date)::date AS month,
    ROUND(SUM(oi.unit_price * oi.quantity)::numeric, 2) AS total_revenue
FROM orders o
JOIN order_items oi ON o.order_id:: bigint = oi.order_id::bigint
GROUP BY month
ORDER BY month;

-- Average Order Value (AOV) Segmentation
/* How does the average order value vary by customer type, region, or product category */
SELECT 
    p.category,
    ROUND(SUM(oi.unit_price * oi.quantity)::numeric / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
GROUP BY p.category
ORDER BY avg_order_value DESC;


-- statstical summary
drop view  if exists vw_statics;
create or replace view vw_statics as
SELECT 
    ROUND(MAX(order_amount)::numeric, 2) AS max_order,
    ROUND(MIN(order_amount)::numeric, 2) AS min_order,
    ROUND(AVG(order_amount)::numeric, 2) AS avg_order,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY order_amount) AS median_order,
    ROUND(STDDEV(order_amount)::numeric, 2) AS std_order,
    ROUND(SUM(order_amount)::numeric, 2) AS total_order_value,
    COUNT(order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    ROUND(((STDDEV(order_amount) / AVG(order_amount)) * 100)::numeric, 2) AS cv_percent, --Coefficient of Variation (CV)
    ( 
      SELECT order_amount
      FROM orders
      GROUP BY order_amount
      ORDER BY COUNT(*) DESC
      LIMIT 1
    ) AS mode_order
FROM orders;


-- Example: probability (fraction) of COD orders
SELECT 
  SUM(CASE WHEN payment_method='COD' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS prob_cod
FROM orders;


-- 1.Customer RFM (Recency, Frequency, Monetary)
/* how many days has it been since purchase
 how frequently customer brought the product
 what is the total spend by the customer
- Helps identify VIP customers vs at-risk customers.
Business Value:
Supports targeted retention campaigns and loyalty programs.*/
CREATE OR REPLACE VIEW vw_customer_rfm AS
WITH last_purchase AS (
  SELECT 
    customer_id,
    MAX(order_date) AS last_orde,
    COUNT(*) AS frequency,
    SUM(order_amount) AS monetary
  FROM orders
  GROUP BY customer_id
)
SELECT 
  c.customer_id,
  c.name,
  (CURRENT_DATE - last_order) as recency,  
  frequency,
  monetary,
  NTILE(5) OVER (ORDER BY (CURRENT_DATE - last_order) DESC) AS r_score, --lower the better so desc
  NTILE(5) OVER (ORDER BY frequency ASC) AS f_score,
  NTILE(5) OVER (ORDER BY monetary ASC) AS m_score
FROM last_purchase lp
JOIN customers c ON lp.customer_id = c.customer_id;


-- 2. Pareto Analysis – Top 10% Customers
/*Solution:
- `vw_top_customers` view ranks customers by total spend.
Business Value:
Prioritizes high-value customers for personalized marketing or premium services.*/
CREATE OR REPLACE VIEW vw_top_customers AS
SELECT 
  customer_id,
  SUM(order_amount) AS total_spend,
  RANK() OVER (ORDER BY SUM(order_amount) DESC) AS rank
FROM orders
GROUP BY customer_id;

-- 3. Product Performance and unique customers
/*Which products are selling the most and to how many unique customers?
Solution: 
- `vw_productcust` view aggregates total revenue, total quantity, and unique buyers per product.
- Top 10 products identified for strategic focus.
Business Value: 
Informs inventory management, promotions, and product portfolio optimization.*/
drop view if exists vw_productcust 
CREATE OR REPLACE VIEW vw_productcust AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(oi.quantity) AS total_quantity,
    ROUND(SUM(oi.unit_price * oi.quantity)::numeric,2) AS total_revenue,
	ROUND(AVG(oi.unit_price * oi.quantity)::numeric,2) AS avg_revenue
FROM order_items oi
JOIN orders o 
    ON oi.order_id = o.order_id
JOIN products p 
    ON oi.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

DROP VIEW IF EXISTS vw_cohort_retention;

4. cohort retention
/*Are customers retained over time? How do cohorts behave month-over-month?
Solution:
- `vw_cohort_retention` view calculates active customers per cohort month.
- Provides retention metrics for each signup cohort.
Business Value:
Helps assess marketing effectiveness and churn risk, 
guiding engagement strategies.*/
CREATE OR REPLACE VIEW vw_cohort_retention AS
WITH cohort_base AS (
  SELECT 
      c.customer_id,
      DATE_TRUNC('month', c.signup_date)::date AS cohort_month,
      DATE_TRUNC('month', o.order_date)::date AS order_month
  FROM customers c
  JOIN orders o 
      ON c.customer_id = o.customer_id
),
cohort_labeled AS (
  SELECT
      cohort_month,
      order_month,
      COUNT(DISTINCT customer_id) AS active_customers
  FROM cohort_base
  GROUP BY cohort_month, order_month
),
cohort_size AS (
  SELECT 
      DATE_TRUNC('month', signup_date)::date AS cohort_month,
      COUNT(DISTINCT customer_id) AS total_customers
  FROM customers
  GROUP BY DATE_TRUNC('month', signup_date)::date
),
filtered_result AS (
  SELECT
      c.cohort_month,
      c.active_customers,
      ROUND(100.0 * c.active_customers / cs.total_customers, 2) AS retention_rate,
      (EXTRACT(YEAR FROM c.order_month) - EXTRACT(YEAR FROM c.cohort_month)) * 12 +
      (EXTRACT(MONTH FROM c.order_month) - EXTRACT(MONTH FROM c.cohort_month)) + 1 AS month_number
  FROM cohort_labeled c
  JOIN cohort_size cs USING (cohort_month)
)
SELECT
  cohort_month,
  active_customers,
  retention_rate
FROM filtered_result
WHERE month_number = 1
ORDER BY cohort_month;











