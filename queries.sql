-- =====================================================================
-- Retail SQL Analytics — 10 analytical queries against the retail schema.
--
-- Each query is self-contained (no temporary tables). Comments above each
-- block explain the business question and the SQL features used.
--
-- Run with: docker compose exec -T postgres psql -U analyst -d retail < queries.sql
-- Or interactively: docker compose exec postgres psql -U analyst -d retail
-- =====================================================================


-- ---------------------------------------------------------------------
-- Q01 - Top 10 customers by lifetime spend
-- ---------------------------------------------------------------------
-- Business question: who are the most valuable customers?
-- Concepts: INNER JOIN across 3 tables, GROUP BY, aggregation with NUMERIC,
--           filtering on order status (only delivered counts as revenue).
-- ---------------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.country,
    COUNT(DISTINCT o.order_id)                          AS order_count,
    SUM(oi.quantity * oi.unit_price - oi.discount)::NUMERIC(12,2) AS lifetime_revenue
FROM customers c
JOIN orders o        ON o.customer_id = c.customer_id
JOIN order_items oi  ON oi.order_id = o.order_id
WHERE o.status IN ('delivered', 'shipped')
GROUP BY c.customer_id, c.full_name, c.country
ORDER BY lifetime_revenue DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- Q02 - Monthly revenue with running total
-- ---------------------------------------------------------------------
-- Business question: what's the monthly revenue trajectory and total-to-date?
-- Concepts: CTE (WITH), DATE_TRUNC, SUM(...) OVER (...) running total,
--           ROUND for presentation.
-- ---------------------------------------------------------------------
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', o.order_date)::DATE AS month,
        SUM(oi.quantity * oi.unit_price - oi.discount) AS monthly_revenue
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status IN ('delivered', 'shipped')
    GROUP BY DATE_TRUNC('month', o.order_date)
)
SELECT
    month,
    ROUND(monthly_revenue, 2) AS monthly_revenue,
    ROUND(SUM(monthly_revenue) OVER (ORDER BY month), 2) AS running_total
FROM monthly
ORDER BY month;


-- ---------------------------------------------------------------------
-- Q03 - Top product per category by revenue
-- ---------------------------------------------------------------------
-- Business question: which product is the bestseller in each category?
-- Concepts: DENSE_RANK () OVER (PARTITION BY ... ORDER BY ...),
--           filtering after ranking with an outer query.
-- Why DENSE_RANK over ROW_NUMBER: we want to surface ties cleanly.
-- ---------------------------------------------------------------------
WITH product_revenue AS (
    SELECT
        cat.category_id,
        cat.name        AS category_name,
        p.product_id,
        p.name          AS product_name,
        SUM(oi.quantity * oi.unit_price - oi.discount) AS revenue
    FROM order_items oi
    JOIN products p     ON p.product_id = oi.product_id
    JOIN categories cat ON cat.category_id = p.category_id
    JOIN orders o       ON o.order_id = oi.order_id
    WHERE o.status IN ('delivered', 'shipped')
    GROUP BY cat.category_id, cat.name, p.product_id, p.name
),
ranked AS (
    SELECT
        category_name,
        product_name,
        ROUND(revenue, 2) AS revenue,
        DENSE_RANK() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rnk
    FROM product_revenue
)
SELECT category_name, product_name, revenue
FROM ranked
WHERE rnk = 1
ORDER BY revenue DESC;


-- ---------------------------------------------------------------------
-- Q04 - Customer cohort retention by signup month
-- ---------------------------------------------------------------------
-- Business question: how many customers from each signup cohort are still
-- ordering N months after signup?
-- Concepts: multi-CTE pipeline, DATE_TRUNC, generating cohort/order pairs,
--           AGE() arithmetic via period subtraction, conditional aggregation.
-- ---------------------------------------------------------------------
WITH cohorts AS (
    SELECT
        c.customer_id,
        DATE_TRUNC('month', c.signup_date)::DATE AS cohort_month
    FROM customers c
),
order_months AS (
    SELECT
        o.customer_id,
        DATE_TRUNC('month', o.order_date)::DATE AS order_month
    FROM orders o
    WHERE o.status IN ('delivered', 'shipped')
),
cohort_orders AS (
    SELECT
        c.cohort_month,
        c.customer_id,
        (EXTRACT(YEAR  FROM AGE(om.order_month, c.cohort_month)) * 12 +
         EXTRACT(MONTH FROM AGE(om.order_month, c.cohort_month)))::INT AS months_since_signup
    FROM cohorts c
    JOIN order_months om ON om.customer_id = c.customer_id
)
SELECT
    cohort_month,
    COUNT(DISTINCT customer_id) FILTER (WHERE months_since_signup = 0)  AS m0,
    COUNT(DISTINCT customer_id) FILTER (WHERE months_since_signup = 1)  AS m1,
    COUNT(DISTINCT customer_id) FILTER (WHERE months_since_signup = 3)  AS m3,
    COUNT(DISTINCT customer_id) FILTER (WHERE months_since_signup = 6)  AS m6,
    COUNT(DISTINCT customer_id) FILTER (WHERE months_since_signup = 12) AS m12
FROM cohort_orders
GROUP BY cohort_month
ORDER BY cohort_month;


-- ---------------------------------------------------------------------
-- Q05 - Lapsed customers (no order in last 90 days)
-- ---------------------------------------------------------------------
-- Business question: who should the win-back campaign target?
-- Concepts: NOT EXISTS subquery, INTERVAL date math, anti-join semantics
--           (correct way to express "customers without recent orders").
-- ---------------------------------------------------------------------
WITH most_recent_order_date AS (
    SELECT MAX(order_date) AS d FROM orders
)
SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.country,
    (SELECT MAX(o.order_date)
     FROM orders o
     WHERE o.customer_id = c.customer_id) AS last_order_date
FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)
  AND NOT EXISTS (
      SELECT 1 FROM orders o
      WHERE o.customer_id = c.customer_id
        AND o.order_date >= (SELECT d FROM most_recent_order_date) - INTERVAL '90 days'
  )
ORDER BY last_order_date ASC
LIMIT 25;


-- ---------------------------------------------------------------------
-- Q06 - Pareto analysis: cumulative revenue share by customer rank
-- ---------------------------------------------------------------------
-- Business question: what % of customers drive 80% of revenue?
-- Concepts: cumulative SUM(...) OVER (ORDER BY ...) for the running %,
--           ROW_NUMBER for rank, division by overall SUM.
-- ---------------------------------------------------------------------
WITH customer_revenue AS (
    SELECT
        c.customer_id,
        SUM(oi.quantity * oi.unit_price - oi.discount) AS revenue
    FROM customers c
    JOIN orders o        ON o.customer_id = c.customer_id
    JOIN order_items oi  ON oi.order_id = o.order_id
    WHERE o.status IN ('delivered', 'shipped')
    GROUP BY c.customer_id
),
ranked AS (
    SELECT
        customer_id,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS customer_rank,
        SUM(revenue) OVER ()                       AS total_revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC ROWS UNBOUNDED PRECEDING) AS cumulative_revenue
    FROM customer_revenue
)
SELECT
    customer_rank,
    customer_id,
    ROUND(revenue, 2)             AS revenue,
    ROUND(cumulative_revenue, 2)  AS cumulative_revenue,
    ROUND(100.0 * cumulative_revenue / total_revenue, 2) AS cumulative_pct,
    ROUND(100.0 * customer_rank / COUNT(*) OVER (), 2)   AS rank_pct
FROM ranked
ORDER BY customer_rank;


-- ---------------------------------------------------------------------
-- Q07 - First-order vs subsequent-order basket size
-- ---------------------------------------------------------------------
-- Business question: do customers spend more on their first order than later?
-- Concepts: ROW_NUMBER per customer to identify the first order, CTE,
--           comparison aggregation by group label.
-- ---------------------------------------------------------------------
WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        SUM(oi.quantity * oi.unit_price - oi.discount) AS order_value,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date) AS seq
    FROM orders o
    JOIN order_items oi ON oi.order_id = o.order_id
    WHERE o.status IN ('delivered', 'shipped')
    GROUP BY o.order_id, o.customer_id, o.order_date
)
SELECT
    CASE WHEN seq = 1 THEN 'first_order' ELSE 'subsequent_orders' END AS bucket,
    COUNT(*)                 AS n_orders,
    ROUND(AVG(order_value), 2) AS avg_basket,
    ROUND(STDDEV(order_value), 2) AS stddev_basket
FROM order_totals
GROUP BY CASE WHEN seq = 1 THEN 'first_order' ELSE 'subsequent_orders' END
ORDER BY bucket;


-- ---------------------------------------------------------------------
-- Q08 - Monthly order-status mix (delivery rate, cancellation rate)
-- ---------------------------------------------------------------------
-- Business question: is order completion rate trending up or down?
-- Concepts: FILTER (WHERE ...) clause for conditional aggregation —
--           cleaner than nested CASE expressions.
-- ---------------------------------------------------------------------
SELECT
    DATE_TRUNC('month', order_date)::DATE AS month,
    COUNT(*)                                                       AS total_orders,
    COUNT(*) FILTER (WHERE status = 'delivered')                   AS delivered,
    COUNT(*) FILTER (WHERE status = 'cancelled')                   AS cancelled,
    COUNT(*) FILTER (WHERE status = 'returned')                    AS returned,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'delivered') / COUNT(*), 2) AS delivered_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'cancelled') / COUNT(*), 2) AS cancelled_pct
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;


-- ---------------------------------------------------------------------
-- Q09 - RFM customer segmentation (Recency / Frequency / Monetary)
-- ---------------------------------------------------------------------
-- Business question: bucket every customer into a 5x5x5 RFM grid for
-- targeted marketing.
-- Concepts: NTILE for equal-frequency bucketing, multi-CTE pipeline,
--           composite RFM string.
-- ---------------------------------------------------------------------
WITH rfm_base AS (
    SELECT
        c.customer_id,
        c.full_name,
        MAX(o.order_date)::DATE AS last_order_date,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(oi.quantity * oi.unit_price - oi.discount) AS monetary
    FROM customers c
    JOIN orders o        ON o.customer_id = c.customer_id
    JOIN order_items oi  ON oi.order_id = o.order_id
    WHERE o.status IN ('delivered', 'shipped')
    GROUP BY c.customer_id, c.full_name
),
rfm_scored AS (
    SELECT
        customer_id,
        full_name,
        last_order_date,
        frequency,
        ROUND(monetary, 2) AS monetary,
        -- Recency: more recent date = better, so we flip the order direction
        NTILE(5) OVER (ORDER BY last_order_date DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)       AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)        AS m_score
    FROM rfm_base
)
SELECT
    customer_id,
    full_name,
    last_order_date,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CONCAT(r_score, f_score, m_score) AS rfm_segment,
    CASE
        WHEN r_score = 1 AND f_score = 1 AND m_score = 1 THEN 'Champions'
        WHEN r_score = 1 AND f_score <= 2                THEN 'Loyal'
        WHEN r_score = 1                                  THEN 'Recent'
        WHEN r_score >= 4 AND f_score = 1                THEN 'At Risk'
        WHEN r_score = 5                                  THEN 'Lapsed'
        ELSE 'Other'
    END AS segment
FROM rfm_scored
ORDER BY r_score, f_score, m_score
LIMIT 30;


-- ---------------------------------------------------------------------
-- Q10 - Underperforming products (active but no sales in last 6 months)
-- ---------------------------------------------------------------------
-- Business question: which active SKUs should we discontinue or promote?
-- Concepts: LEFT JOIN ... IS NULL anti-join pattern, COALESCE for the
--           "no orders" case, INTERVAL date math.
-- ---------------------------------------------------------------------
WITH most_recent AS (SELECT MAX(order_date) AS d FROM orders),
recent_sales AS (
    SELECT
        oi.product_id,
        SUM(oi.quantity * oi.unit_price - oi.discount) AS revenue_6mo
    FROM order_items oi
    JOIN orders o ON o.order_id = oi.order_id
    WHERE o.status IN ('delivered', 'shipped')
      AND o.order_date >= (SELECT d FROM most_recent) - INTERVAL '6 months'
    GROUP BY oi.product_id
)
SELECT
    p.sku,
    p.name AS product_name,
    cat.name AS category_name,
    p.list_price,
    COALESCE(rs.revenue_6mo, 0) AS revenue_last_6_months,
    p.is_active
FROM products p
JOIN categories cat ON cat.category_id = p.category_id
LEFT JOIN recent_sales rs ON rs.product_id = p.product_id
WHERE p.is_active = TRUE
  AND COALESCE(rs.revenue_6mo, 0) = 0
ORDER BY p.list_price DESC;
