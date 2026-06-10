-- ═══════════════════════════════════════════════════════════════
-- Gold-layer KPI Queries
-- Databricks PySpark Lakehouse Pipeline
-- ═══════════════════════════════════════════════════════════════

-- ───────────────────────────────────────────────────────────────
-- 1. Revenue overview: last 30 days vs. previous 30 days
-- ───────────────────────────────────────────────────────────────
SELECT
    SUM(CASE WHEN order_date >= CURRENT_DATE - 30 THEN revenue END)      AS revenue_last_30d,
    SUM(CASE WHEN order_date BETWEEN CURRENT_DATE - 60
                              AND CURRENT_DATE - 31 THEN revenue END)    AS revenue_prev_30d,
    ROUND(
        (SUM(CASE WHEN order_date >= CURRENT_DATE - 30 THEN revenue END)
         - SUM(CASE WHEN order_date BETWEEN CURRENT_DATE - 60
                                    AND CURRENT_DATE - 31 THEN revenue END))
        / NULLIF(SUM(CASE WHEN order_date BETWEEN CURRENT_DATE - 60
                                          AND CURRENT_DATE - 31 THEN revenue END), 0) * 100,
        2
    )                                                                     AS wow_growth_pct
FROM gold_daily_sales;

-- ───────────────────────────────────────────────────────────────
-- 2. Month-over-month revenue trend (last 12 months)
-- ───────────────────────────────────────────────────────────────
SELECT
    year_month,
    orders,
    revenue,
    revenue_prev_month,
    mom_growth_pct,
    SUM(revenue) OVER (ORDER BY order_year, order_month
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS cumulative_revenue
FROM gold_monthly_revenue
ORDER BY order_year DESC, order_month DESC
LIMIT 12;

-- ───────────────────────────────────────────────────────────────
-- 3. Top 10 products by revenue with revenue share
-- ───────────────────────────────────────────────────────────────
SELECT
    revenue_rank,
    product_name,
    category,
    units_sold,
    revenue,
    avg_discount_rate,
    CONCAT(revenue_share_pct, '%')  AS revenue_share
FROM gold_product_performance
ORDER BY revenue_rank
LIMIT 10;

-- ───────────────────────────────────────────────────────────────
-- 4. Customer value segmentation distribution
-- ───────────────────────────────────────────────────────────────
SELECT
    value_segment,
    COUNT(*)                              AS customer_count,
    ROUND(AVG(customer_lifetime_value), 2) AS avg_clv,
    ROUND(SUM(customer_lifetime_value), 2) AS total_clv,
    ROUND(SUM(customer_lifetime_value)
          / SUM(SUM(customer_lifetime_value)) OVER () * 100, 1) AS pct_of_total_revenue
FROM gold_customer_lifetime_value
GROUP BY value_segment
ORDER BY avg_clv DESC;

-- ───────────────────────────────────────────────────────────────
-- 5. Top 20 customers by lifetime value
-- ───────────────────────────────────────────────────────────────
SELECT
    customer_id,
    customer_name,
    city,
    country,
    completed_orders,
    customer_lifetime_value,
    value_segment,
    first_order_date,
    last_order_date,
    DATEDIFF(last_order_date, first_order_date)  AS customer_tenure_days
FROM gold_customer_lifetime_value
ORDER BY customer_lifetime_value DESC
LIMIT 20;

-- ───────────────────────────────────────────────────────────────
-- 6. Return rate by category (quality signal)
-- ───────────────────────────────────────────────────────────────
SELECT
    category,
    total_orders,
    returned_orders,
    CONCAT(return_rate_pct, '%')   AS return_rate,
    total_revenue,
    returned_revenue,
    ROUND(returned_revenue / total_revenue * 100, 2) AS revenue_at_risk_pct
FROM gold_returns_analysis
ORDER BY return_rate_pct DESC;

-- ───────────────────────────────────────────────────────────────
-- 7. Channel performance: revenue, AOV, order share
-- ───────────────────────────────────────────────────────────────
SELECT
    channel,
    orders,
    revenue,
    avg_order_value,
    ROUND(orders / SUM(orders) OVER () * 100, 1)    AS order_share_pct,
    ROUND(revenue / SUM(revenue) OVER () * 100, 1)  AS revenue_share_pct
FROM gold_channel_performance
ORDER BY revenue DESC;

-- ───────────────────────────────────────────────────────────────
-- 8. Revenue by country (for geographic dashboards)
-- ───────────────────────────────────────────────────────────────
SELECT
    country,
    COUNT(DISTINCT customer_id)             AS customers,
    SUM(completed_orders)                   AS total_orders,
    ROUND(SUM(customer_lifetime_value), 2)  AS total_revenue,
    ROUND(AVG(customer_lifetime_value), 2)  AS avg_clv_per_customer
FROM gold_customer_lifetime_value
GROUP BY country
ORDER BY total_revenue DESC;
