CREATE TABLE bronze.crm_cust_info(
    cst_id NVARCHAR(7),
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr VARCHAR(1),
    cst_create_date DATE
);

CREATE TABLE bronze.crm_prd_info(
    prd_id NVARCHAR(3),
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(10),
    prd_start_dt DATE,
    prd_end_dt DATE
);

CREATE TABLE bronze.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id NVARCHAR(7),
    sls_order_date BIGINT,
    sls_ship_date BIGINT,
    sls_due_date BIGINT,
    sls_sales BIGINT,
    sls_quantity INT,
    sls_price BIGINT
);

CREATE TABLE bronze.erp_cust_az12(
    cid NVARCHAR(50),
    bdate DATE,
    gen VARCHAR(6)
);

CREATE TABLE bronze.erp_loc_a101(
  cid NVARCHAR(50),
  cntry NVARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2(
  id NVARCHAR(50),
  cat NVARCHAR(100),
  subcat NVARCHAR(200),
  maintenance NVARCHAR(3)
);

-- BULK INSERT :
-- bronze.crm_cust_info = 18494
CREATE OR ALTER PROCEDURE bronze.load_bronze
    AS BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME
        BEGIN TRY
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_cust_info;
            BULK INSERT bronze.crm_cust_info
            FROM  'D:\KULIAH\KULIAH\BELAJAR MANDIRI\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '=========================================================================================================='


            -- bronze.crm_prd_info = 397
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_prd_info;
            BULK INSERT bronze.crm_prd_info
            FROM  'D:\KULIAH\KULIAH\BELAJAR MANDIRI\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '=========================================================================================================='

            -- bronze.crm_sales_details = 60398
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.crm_sales_details;
            BULK INSERT bronze.crm_sales_details
            FROM  'D:\KULIAH\KULIAH\BELAJAR MANDIRI\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '=========================================================================================================='


            -- bronze.erp_cust_az12 = 18484
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_cust_az12
            BULK INSERT bronze.erp_cust_az12
            FROM 'D:\KULIAH\KULIAH\BELAJAR MANDIRI\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '=========================================================================================================='


            -- bronze.erp_loc_a101 = 18484
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_loc_a101
            BULK INSERT bronze.erp_loc_a101
            FROM 'D:\KULIAH\KULIAH\BELAJAR MANDIRI\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );
            SET @end_time = GETDATE();
            PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '=========================================================================================================='


            --  bronze.erp_px_cat_g1v2 = 37
            SET @start_time = GETDATE();
            TRUNCATE TABLE bronze.erp_px_cat_g1v2
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'D:\KULIAH\KULIAH\BELAJAR MANDIRI\DATA WAREHOUSE\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = ',',
                TABLOCK
                );

            SET @end_time = GETDATE();
            PRINT 'Load Duration : ' + CAST (DATEDIFF(SECOND , @start_time, @end_time) AS NVARCHAR) + 'Seconds';
            PRINT '=========================================================================================================='


        END TRY
        BEGIN CATCH
            PRINT '============================================================================================================='
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
            PRINT 'ERROR MESSAGE' + ERROR_MESSAGE()
            PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR)
            PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR)
            PRINT '============================================================================================================='
        END CATCH
    END;