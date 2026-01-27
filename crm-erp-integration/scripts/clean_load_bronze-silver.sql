-- crm_cust_info
-- check null and duplicate at primary key
-- at cst_id
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- using newest data
SELECT * FROM
    (   SELECT *, ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flas_last
        FROM bronze.crm_cust_info
    )t WHERE flas_last = 1

SELECT * FROM
    (   SELECT *, ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flas_last
        FROM silver.crm_cust_info
    )t WHERE flas_last = 1


-- check for unwanted spaces in str value
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


--=================================--=================================================--=============================================--==================
-- crm_prd_info
SELECT * FROM bronze.crm_prd_info

-- check null or duplicates
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- check null and negative cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- check for unwanted spaces in str value
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- check invalid date order
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- end date - start date
-- 1. start date must earlier then end date --> switch end date and start date
-- 2. each record must have start date --> end date = start date at next record - 1 day
-- 3. the dates are overlapping

SELECT prd_id, prd_key, prd_nm, prd_start_dt, prd_end_dt,
      DATEADD( DAY, -1, LEAD(prd_start_dt)  OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
    ) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

--==========================================================================--======================================================================================--============
-- crm_sales_details
SELECT *
FROM bronze.crm_sales_details

-- check unwanted spaces
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)

-- check foreign key
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (
    SELECT prd_key
    FROM silver.crm_prd_info)

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (
    SELECT cst_id
    FROM silver.crm_cust_info)

-- check invalid date
SELECT sls_ord_num, sls_order_date
FROM silver.crm_sales_details
WHERE sls_order_date <= 0 OR len(sls_order_date) != 8

SELECT sls_ord_num, sls_ship_date
FROM bronze.crm_sales_details
WHERE sls_ship_date <= 0 OR len(sls_ship_date) != 8

-- order date must earlier then shipping date or due date
SELECT sls_order_date, sls_ship_date, sls_due_date
FROM silver.crm_sales_details
WHERE sls_order_date > sls_ship_date OR sls_order_date > sls_due_date

-- data consistency : sales, quantity and price
-- sales = quantity * price
-- values must not be null, zero or negative

SELECT DISTINCT sls_sales, sls_quantity , sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales <= 0 OR sls_price <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

/* rule :
1. if sales negative/null/zero derive it using quantity * price
2. if price zero or null derive it using sales / quantity
3. if price negative, convert into positive
*/

--=========================================================================================--=================================================================================--
SELECT * FROM bronze.erp_cust_az12;

-- data consistency
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
    ELSE cid
    END
    NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- data standardization and consistency
SELECT DISTINCT
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12

--================================================================================--======================================================================
SELECT *
FROM bronze.erp_loc_a101

-- data consistency and standardization
SELECT REPLACE(cid, '-', '') as cid_new
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info)


SELECT
    REPLACE(cid, '-', '') as cid,
    CASE
        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
        WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
        WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
    ELSE (TRIM(cntry))
    END AS cntry
FROM bronze.erp_loc_a101

--========================================================================================================--============================================================
SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM silver.crm_prd_info

-- check unwanted space
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id != TRIM(id) OR cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2