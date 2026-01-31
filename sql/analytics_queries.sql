-- =========================================
-- Analytics Queries: Car Sales
-- =========================================

-- 1) Total revenue, total units, total transactions (overall)
SELECT
  COUNT(*) AS total_transactions,
  SUM(quantity) AS total_units_sold,
  SUM(total_amount) AS total_revenue
FROM sales;

-- 2) Revenue by month
SELECT
  DATE_TRUNC('month', sale_date) AS month,
  SUM(total_amount) AS revenue,
  SUM(quantity) AS units_sold,
  COUNT(*) AS transactions
FROM sales
GROUP BY 1
ORDER BY 1;

-- 3) Top 10 brands by revenue
SELECT
  c.brand,
  SUM(s.total_amount) AS revenue,
  SUM(s.quantity) AS units_sold
FROM sales s
JOIN cars c ON c.car_id = s.car_id
GROUP BY c.brand
ORDER BY revenue DESC
LIMIT 10;

-- 4) Top 10 models by revenue
SELECT
  c.brand,
  c.model,
  c.model_year,
  SUM(s.total_amount) AS revenue,
  SUM(s.quantity) AS units_sold
FROM sales s
JOIN cars c ON c.car_id = s.car_id
GROUP BY c.brand, c.model, c.model_year
ORDER BY revenue DESC
LIMIT 10;

-- 5) Revenue by country (dealer location)
SELECT
  COALESCE(d.country, 'unknown') AS country,
  SUM(s.total_amount) AS revenue,
  SUM(s.quantity) AS units_sold
FROM sales s
JOIN dealers d ON d.dealer_id = s.dealer_id
GROUP BY 1
ORDER BY revenue DESC;

-- 6) Revenue by city (top 15)
SELECT
  COALESCE(d.city, 'unknown') AS city,
  COALESCE(d.country, 'unknown') AS country,
  SUM(s.total_amount) AS revenue
FROM sales s
JOIN dealers d ON d.dealer_id = s.dealer_id
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 15;

-- 7) Average selling price (ASP) by brand (weighted by quantity)
SELECT
  c.brand,
  ROUND(SUM(s.total_amount) / NULLIF(SUM(s.quantity), 0), 2) AS avg_selling_price
FROM sales s
JOIN cars c ON c.car_id = s.car_id
GROUP BY c.brand
ORDER BY avg_selling_price DESC;

-- 8) Category performance
SELECT
  COALESCE(c.category, 'unknown') AS category,
  SUM(s.total_amount) AS revenue,
  SUM(s.quantity) AS units_sold,
  COUNT(*) AS transactions
FROM sales s
JOIN cars c ON c.car_id = s.car_id
GROUP BY 1
ORDER BY revenue DESC;

-- 9) YoY revenue comparison (by year)
SELECT
  EXTRACT(YEAR FROM sale_date)::INT AS year,
  SUM(total_amount) AS revenue,
  SUM(quantity) AS units_sold
FROM sales
GROUP BY 1
ORDER BY 1;

-- 10) Best month per brand (ranked)
WITH brand_month AS (
  SELECT
    c.brand,
    DATE_TRUNC('month', s.sale_date) AS month,
    SUM(s.total_amount) AS revenue
  FROM sales s
  JOIN cars c ON c.car_id = s.car_id
  GROUP BY 1, 2
),
ranked AS (
  SELECT
    brand,
    month,
    revenue,
    DENSE_RANK() OVER (PARTITION BY brand ORDER BY revenue DESC) AS rnk
  FROM brand_month
)
SELECT brand, month, revenue
FROM ranked
WHERE rnk = 1
ORDER BY revenue DESC;
