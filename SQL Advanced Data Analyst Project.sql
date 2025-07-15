-- CUMULATIVE ANALYSIS
-- CHANGE OVER TIME ANALYSIS | [MEASURE] BY [DATE DIMENSION]


-- TOTAL SALES OVER THE TIME(YEARS)
SELECT 
YEAR(order_date) AS order_year,
SUM(sales_amount) AS total_Sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM [DataWarehouseAnalytics].[gold].[fact_sales]
WHERE order_date IS NOT NULL
GROUP BY  YEAR(order_date)
ORDER BY YEAR(order_date)


-- TOTAL SALES OVER THE TIME(MONTHS)
SELECT 
YEAR(order_date) AS order_year,
MONTH(order_date) AS order_month,
SUM(sales_amount) AS total_Sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM [DataWarehouseAnalytics].[gold].[fact_sales]
WHERE order_date IS NOT NULL
GROUP BY  YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date)

-- BY USING DATETRUNC() FUNCTION [MONTH]
SELECT 
DATETRUNC(MONTH, order_date) AS order_date,
SUM(sales_amount) AS total_Sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM [DataWarehouseAnalytics].[gold].[fact_sales]
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date)

-- BY USING DATETRUNC() FUNCTION [YEAR]
SELECT 
DATETRUNC(YEAR, order_date) AS order_date,
SUM(sales_amount) AS total_Sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM [DataWarehouseAnalytics].[gold].[fact_sales]
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(YEAR, order_date)
ORDER BY DATETRUNC(YEAR, order_date)

-- BY USING FORMAT() FUNCTION
SELECT 
FORMAT(order_date, 'yyyy-MMM' ) AS order_date,
SUM(sales_amount) AS total_Sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM [DataWarehouseAnalytics].[gold].[fact_sales]
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM' )
ORDER BY FORMAT(order_date, 'yyyy-MMM' )




-- AGGREGATE THE DATA PROGRESSIVELY OVER TIME | [CUMULATIVE MEASURE] BY [DATE DIMENSION]

-- CALCULATE THE TOTAL SALES PER MONTH
-- AND RUNNING TOTAL OF SALES OVER TIME
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (PARTITION BY order_date ORDER BY order_date) AS running_total_sales
FROM
	(
	SELECT
	DATETRUNC(month, order_date) AS order_date,
	SUM(sales_amount) AS total_sales
	FROM [DataWarehouseAnalytics].[gold].[fact_sales]
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
	) t



-- RUNNING TOTAL OF SALES OVER THE MONTHS
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS Runnin_Total_Sales
FROM
	(
	SELECT
	DATETRUNC(YEAR, order_date) AS order_date,
	SUM(Sales_amount) AS total_sales
	FROM [DataWarehouseAnalytics].[gold].[fact_sales]
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(YEAR, order_date)
	)P



-- MOVING AVERAGE SALES & PRICE OVER TIME
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS Runnin_Total_Sales,
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
	(
	SELECT
	DATETRUNC(YEAR, order_date) AS order_date,
	SUM(Sales_amount) AS total_sales,
	AVG(price) AS avg_price
	FROM [DataWarehouseAnalytics].[gold].[fact_sales]
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(YEAR, order_date)
	)M



-- PERFORMANCE ANALYSIS
-- COMPARING THE CURRENT VALUE TO A TARGET VALUE CURRENT[MEASURE] - TARGET[MEASURE]


-- ANALYZE THE YEARLY PERFORMANCE OF PRODUCTS BY COMPARING EACH PRODUCT'S SALES TO 
-- BOTH IT'S AVERAGE SALES PERFORMANCE AND THE REVIOUS YEAR'S SALES 
WITH yearly_producvt_sales AS (
SELECT
YEAR(f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM [DataWarehouseAnalytics].[gold].[fact_sales] f
LEFT JOIN [DataWarehouseAnalytics].[gold].[dim_products] p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
GROUP BY 
YEAR(f.order_date),
p.product_name
)

SELECT
order_year,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) avg_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY Product_name) AS diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY Product_name) > 0 THEN 'Above Avg'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY Product_name) < 0 THEN 'Below Avg'
	 ELSE 'Avg'
	 END avg_change,
-- YEAR OVER YEAR ANALYSIS
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) py_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
	 ELSE 'No Change'
	 END avg_change
FROM yearly_producvt_sales


-- MONTH OVER MONTH ANALYSIS
WITH monthly_product_sales AS (
SELECT
MONTH(f.order_date) AS order_month,
p.product_name,
SUM(f.sales_amount) AS current_sales
FROM [DataWarehouseAnalytics].[gold].[fact_sales] f
LEFT JOIN [DataWarehouseAnalytics].[gold].[dim_products] p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
GROUP BY 
MONTH(f.order_date),
p.product_name
)

SELECT
order_month,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) avg_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY Product_name) AS diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY Product_name) > 0 THEN 'Above Avg'
	 WHEN current_sales - AVG(current_sales) OVER (PARTITION BY Product_name) < 0 THEN 'Below Avg'
	 ELSE 'Avg'
	 END avg_change,
-- MONTH OVER MONTH ANALYSIS
LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) py_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) AS diff_py,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) > 0 THEN 'Increase'
	 WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_month) < 0 THEN 'Decrease'
	 ELSE 'No Change'
	 END avg_change
FROM monthly_product_sales



-- PART-TO-WHOLE ANALYSIS
-- ([MEASURE] / TOTAL[MEASURE]) * 100 BY [DIMENSION]


-- WHICH CATEGORY CONTRIBUTES THE MOST OVERALL SALES
WITH category_sales AS (
SELECT
category,
SUM(sales_amount) AS total_sales
FROM [DataWarehouseAnalytics].[gold].[dim_products] p 
LEFT JOIN [DataWarehouseAnalytics].[gold].[fact_sales] f
ON p.product_key = f.product_key
WHERE sales_amount IS NOT NULL 
GROUP BY category)

SELECT
category,
total_sales,
SUM(total_sales) OVER () overall_sales,
CONCAT(ROUND((CAST (total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2), '%') AS percentage_of_total
FROM  category_sales
ORDER BY total_sales DESC



-- DATA SEGMENTATION 
-- GROUP THE DATA BASED ON A SPECIFIC RANGE 
-- [MEASURE] BY [MEASURE] BY USING CASE WHEN STATEMENT


-- SEGMENT PRODUCTS INTO COST RANGES AND COUNT HOW MANY PRODUCTS FALL INTO EACH SEGMENT
WITH product_segment AS (
SELECT
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	 WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	 ELSE 'Above 1000'
END AS cost_range
FROM [DataWarehouseAnalytics].[gold].[dim_products])

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY  total_products



/* GROUP CUSTOMERS INTO THREE SEGMENTS BASED ON THEIR SPENDING BEHAVIOR
   VIP:			ATLEAST 12 MONTHS OF HISTORY AND SPENDING MORE THEN 5,000
  REGULAR:		ATLEAST 12 MONTHS OF HISTORY BUT SPENDING 5,000 OR LESS
   NEW:			LIFESPAN LESS THEN 12 MONTHS
 */
WITH customer_spending AS(

SELECT
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order,
MAX(order_date) AS last_order,
DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
FROM  [DataWarehouseAnalytics].[gold].[fact_sales] f
LEFT JOIN  [DataWarehouseAnalytics].[gold].[dim_customers] c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key)

SELECT
customer_segment,
COUNT(customer_key) AS total_customers
FROM (
SELECT
customer_key,
CASE
	WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
	ELSE 'New'
END customer_segment
FROM customer_spending) t
GROUP BY customer_segment
ORDER BY total_customers DESC





 /*                                        CUSTOMER REPORT                                                                          
                            ---------------------------------------------------

         PURPOSE:
					-THIS REPORT CONSOLIDATES KEY CUSTOMER METRICS AND BEHAVIORS

			HIGHLIGHTS:
					1. GATHERS ESSENTIAL FIELDS SUCH AS NAMES, AGES AND TRANSACTION DETAILS.
					2. SEGMENTS CUSTOMER INTO CATEGORIES (VIP, REGULAR, NEW) AND AGE GROUPS.
					3. AGGREGATE CUSTOMER-LEVEL METRICS:
						- TOTAL ORDERS
						- TOTAL SALES
						- TOTAL QUANTITY PURCHASED
						- TOTAL PRODUCTS
						- LIFESPAN (IN MONTHS)
					4. CALCULATES VALUABLE KPI's:
						- RECENCY(MONTHS SINCE LAST ORDERS)
						- AVERAGE ORDER VALUE
						- AVERAGE MONTHLY SPEND

 
                           ---------------------------------------------------
			                                     END 
*/

-- 1. BASE QUERY: RETRIEVES CORE COLUMNS FROM TABLES
USE [DataWarehouseAnalytics];
GO
CREATE VIEW gold.report_customers AS
WITH base_query AS (
    SELECT 
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c 
        ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
),
customer_aggregation AS (
    SELECT 
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_product,
        MAX(order_date) AS last_order_date,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY 
        customer_key,
        customer_number,
        customer_name,
        age
)
SELECT 
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and Above'
    END AS age_group,
    CASE
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    last_order_date,
    DATEDIFF(MONTH, last_order_date, GETDATE()) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_product,
    lifespan,
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales * 1.0 / total_orders
    END AS avg_order_value,
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales * 1.0 / lifespan
    END AS avg_monthly_spend
FROM customer_aggregation;

-- QUICK QUERY TO ACCESS CREATED gold.report_customers VIEW ABOVE
SELECT 
customer_segment,
COUNT(customer_number) AS total_customers,
SUM(total_sales) AS total_sales
FROM [DataWarehouseAnalytics].[gold].[report_customers]
GROUP BY customer_segment






/*                                  PRODUCT REPORT                                                                          
                    ---------------------------------------------------

         PURPOSE:
					-THIS REPORT CONSOLIDATES KEY PRODUCT METRICS AND BEHAVIORS

			HIGHLIGHTS:
					1. GATHERS ESSENTIAL FIELDS SUCH AS PRODUCT NAME, CATEGORY, SUBCATEGORY AND COST.
					2. SEGMENTS PRODUCTS BY REVENUE TO IDENTIFY HIGH-PERFORMANCE, MID-RANGE AND LOW-PERFORMERS.
					3. AGGREGATE PRODUCT-LEVEL METRICS:
						- TOTAL ORDERS
						- TOTAL SALES
						- TOTAL QUANTITY SOLD
						- TOTAL CUSTOMERS
						- LIFESPAN (IN MONTHS)
					4. CALCULATES VALUABLE KPI's:
						- RECENCY(MONTHS SINCE LAST SALE)
						- AVERAGE ORDER VALUE (AOR)
						- AVERAGE MONTHLY REVENUE

 
                    ---------------------------------------------------
			                                END 
*/

-- 1. BASE QUERY: RETRIEVES CORE COLUMNS FROM TABLES
-- CREATE AN SQL VIEW TO PROVIDE PRODUCT INSINGHTS
USE [DataWarehouseAnalytics];
GO

CREATE VIEW gold.report_products AS
WITH foundation_query AS 
(
  SELECT 
        f.order_number,
        f.customer_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM [DataWarehouseAnalytics].[gold].[fact_sales] f
    LEFT JOIN [DataWarehouseAnalytics].[gold].[dim_products] p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
   ),
-- 2. PRODUCT AGGREGATION: SUMMARIZE THE KEY METRICS AT THE PRODUCT LEVEL
product_aggregation AS (
SELECT
      product_key,
      product_name,
      category,
      subcategory,
      cost,
      DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
      MAX(order_date) AS last_sales_date,
      COUNT(DISTINCT order_number) AS total_orders,
      COUNT(DISTINCT customer_key) AS total_customers,
      SUM(sales_amount) AS total_sales,
      SUM(quantity) AS total_quantity,
      ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
FROM foundation_query   
GROUP BY 
      product_key,
      product_name,
      category,
      subcategory,
      cost
)
-- 3. FINAL QUERY: COMBINES ALL THE PRODUCTS RESULTS INTO ONE OUTPUT
SELECT
      product_key,
      product_name,
      category,
      subcategory,
      cost,
      DATEDIFF(MONTH, last_sales_date, GETDATE()) AS recency_in_months,
      CASE
           WHEN total_sales > 50000 THEN 'High-Performer'
           WHEN total_sales >= 10000 THEN 'Mid-Range'
           ELSE 'Low-Performer'
      END AS product_segment,
      lifespan,
      total_orders,
      total_sales,
      total_quantity,
      total_customers,
      avg_selling_price,
      -- AVERAGE ORDER REVENUE (AOR)
      CASE 
           WHEN total_orders = 0 THEN 0
           ELSE total_sales / total_orders
      END AS avg_order_revenue,
       -- AVERAGE MONTHLY REVENUE
       CASE
            WHEN lifespan = 0 THEN total_sales
            ELSE total_sales / lifespan
            END AS avg_monthly_revenue
FROM product_aggregation








