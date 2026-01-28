-- Create Dimension Customer
-- using view

-----------------------------------------------------------------------------------------------------------------------

-- CUSTOMER
-- check consistency
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() over (ORDER BY cst_id) AS customer_key,
    cci.cst_id as customer_id,
    cci.cst_key as customer_number,
    cci.cst_firstname as first_name,
    cci.cst_lastname as last_name,
    ela.cntry as country,
    cci.cst_marital_status as marital_status,
     CASE
        WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
        ELSE COALESCE(eca.gen, 'n/a')
    END AS gender,
    eca.bdate as birthdate,
    cci.cst_create_date as create_date
FROM silver.crm_cust_info cci
    LEFT JOIN silver.erp_cust_az12 eca ON eca.cid = cci.cst_key
    LEFT JOIN silver.erp_loc_a101 ela ON ela.cid = cci.cst_key

-- gak konsisten
SELECT DISTINCT
    cci.cst_gndr,
    eca.gen,
    CASE
        WHEN cci.cst_gndr != 'n/a' THEN cci.cst_gndr
        ELSE COALESCE(eca.gen, 'n/a')
    END AS gen_new
FROM silver.crm_cust_info cci
    LEFT JOIN silver.erp_cust_az12 eca ON eca.cid = cci.cst_key
    LEFT JOIN silver.erp_loc_a101 ela ON ela.cid = cci.cst_key

----------------------------------------------------------------------------------------------------------------------------------
-- PRODUCT

CREATE VIEW gold.dim_products AS
SELECT ROW_NUMBER() over (ORDER BY pi.prd_start_dt, pi.prd_key) as product_key,
       pi.prd_id as product_id,
       pi.prd_key as product_number,
       pi.prd_nm as product_name,
       pi.cat_id as category_id,
       pcc.cat as category,
       pcc.subcat as sub_category,
       pcc.maintenance,
       pi.prd_cost as cost,
       pi.prd_line as product_line,
       pi.prd_start_dt as start_date
FROM silver.crm_prd_info pi
    LEFT JOIN silver.erp_px_cat_g1v2 pcc ON pcc.id = pi.cat_id
WHERE pi.prd_end_dt IS NULL;


----------------------------------------------------------------------------------------------------------------
-- facts
-- sales

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_date as order_date,
    sd.sls_ship_date as shipping_date,
    sd.sls_due_date as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers cu ON cu.customer_id = sd.sls_cust_id
LEFT JOIN gold.dim_products pr ON pr.product_number = sd.sls_prd_key