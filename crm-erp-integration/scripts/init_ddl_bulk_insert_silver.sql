CREATE TABLE silver.crm_cust_info(
    cst_id NVARCHAR(10),
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr VARCHAR(10),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.crm_prd_info(
    prd_id NVARCHAR(3),
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id NVARCHAR(7),
    sls_order_date DATE,
    sls_ship_date DATE,
    sls_due_date DATE,
    sls_sales BIGINT,
    sls_quantity INT,
    sls_price BIGINT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen VARCHAR(6),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_loc_a101(
  cid NVARCHAR(50),
  cntry NVARCHAR(50),
  dwh_create_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE silver.erp_px_cat_g1v2(
  id NVARCHAR(50),
  cat NVARCHAR(100),
  subcat NVARCHAR(200),
  maintenance NVARCHAR(3),
  dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- insert
EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver
    AS BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
        BEGIN TRY
        SET @batch_start_time = GETDATE()
        PRINT 'START LOAD SILVER LAYER'


        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : silver.crm_cust_info ';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT 'INSERTING DATA INTO : silver.crm_cust_info ';
        INSERT INTO silver.crm_cust_info
            (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        SELECT
            cst_id, cst_key, TRIM(cst_firstname) as cst_firstname, TRIM(cst_lastname) as cst_lastname,
                CASE
                    WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                    WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                    ELSE 'n/a'
                END cst_material_status,
                CASE
                    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                    ELSE 'n/a'
                END cst_gndr,
            cst_create_date
        FROM
            (   SELECT *, ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flas_last
                FROM bronze.crm_cust_info
            )t WHERE flas_last = 1
        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '=========================================================================================================='

        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : silver.crm_prd_info ';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT 'INSERTING DATA INTO : silver.crm_prd_info ';
        INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT prd_id,
               REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- extract category id
               SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- extrack product key
               prd_nm,
               ISNULL(prd_cost, 0) AS prd_cost,
               CASE UPPER(TRIM(prd_line))
                    WHEN 'M' THEN 'Mountain'
                    WHEN 'R' THEN 'Road'
                    WHEN 'S' THEN 'Other Sale'
                    WHEN 'T' THEN 'Touring'
                    ELSE 'n/a'
               END AS prd_line,                                        -- map product line codes to descriptive values
               prd_start_dt,
               DATEADD( DAY, -1, LEAD(prd_start_dt)  OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
            ) AS prd_end_dt                                            -- calculate end date as one day before the next start date
        FROM bronze.crm_prd_info
        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '=========================================================================================================='


        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : silver.crm_sales_details ';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT 'INSERTING DATA INTO : silver.crm_sales_details ';
        INSERT INTO silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_date, sls_ship_date, sls_due_date, sls_sales, sls_quantity, sls_price)
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
                CASE
                   WHEN sls_order_date = 0 OR LEN(sls_order_date) != 8 THEN NULL
                   ELSE CAST(CAST(sls_order_date AS VARCHAR) AS DATE)
                END AS sls_order_date,
                CASE
                   WHEN sls_ship_date = 0 OR LEN(sls_ship_date) != 8 THEN NULL
                   ELSE CAST(CAST(sls_ship_date AS VARCHAR) AS DATE)
                END AS sls_ship_date,
                CASE
                   WHEN sls_due_date = 0 OR LEN(sls_due_date) != 8 THEN NULL
                   ELSE CAST(CAST(sls_due_date AS VARCHAR) AS DATE)
                END AS sls_due_date,
                CASE
                    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales !=  sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                    ELSE sls_sales
                END AS sls_sales,
            sls_quantity,
                CASE
                    WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
                    ELSE sls_price
                END AS sls_price
        FROM bronze.crm_sales_details
        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '=========================================================================================================='


        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : silver.erp_cust_az12 ';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT 'INSERTING DATA INTO : silver.erp_cust_az12 ';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12
        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '=========================================================================================================='



        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : silver.erp_loc_a101 ';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT 'INSERTING DATA INTO : silver.erp_loc_a101 ';
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') as cid,
            CASE
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'
                WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
            ELSE (TRIM(cntry))
            END AS cntry
        FROM bronze.erp_loc_a101
        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '=========================================================================================================='



        SET @start_time = GETDATE();
        PRINT 'TRUNCATING TABLE : silver.erp_px_cat_g1v2 ';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT 'INSERTING DATA INTO : silver.erp_px_cat_g1v2 ';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2
        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
        PRINT '=========================================================================================================='


        SET @batch_end_time = GETDATE();
        PRINT 'TOTAL LOAD DURATION : ' + CAST (DATEDIFF(SECOND , @batch_start_time, @batch_end_time) AS NVARCHAR) + 'Seconds';

    END TRY
        BEGIN CATCH
            PRINT '============================================================================================================='
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
            PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
            PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR)
            PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR)
            PRINT '============================================================================================================='
        END CATCH
    END