-- ============================================================================
-- Gold Layer Data Quality Checks
-- Purpose: Validate surrogate keys and fact-dimension connectivity
-- Expectation: All queries below should return ZERO rows
-- ============================================================================


-- ============================================================================
-- Check 1: Uniqueness of customer_key in gold.dim_customers
-- Each surrogate key must be unique (dimension primary key rule)
-- ============================================================================

SELECT
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;



-- ============================================================================
-- Check 2: Uniqueness of product_key in gold.dim_products
-- Each product surrogate key must be unique
-- ============================================================================

SELECT
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;



-- ============================================================================
-- Check 3: Fact-to-Dimension Connectivity — gold.fact_sales
-- Validate foreign key integrity between fact and dimension views
-- Ensures every fact row maps to a valid customer and product
--
-- If rows are returned:
--   → Missing dimension records
--   → Broken joins in fact build logic
-- ============================================================================

SELECT
    f.*
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE
    c.customer_key IS NULL
    OR p.product_key IS NULL;
