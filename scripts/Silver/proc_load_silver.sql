/*
===============================================================
Procedure: silver.load_silver
===============================================================

Purpose:
    This procedure loads and transforms data from the bronze layer
    into the silver layer. It performs data cleansing, validation,
    and standardization for both CRM and ERP datasets.

Process Overview:
    1. Truncate existing silver tables
    2. Load cleaned and transformed data from bronze tables
    3. Apply business rules and data quality checks
    4. Handle duplicates using ROW_NUMBER()
    5. Standardize values (e.g., gender, marital status, country)
    6. Fix invalid or inconsistent data (dates, sales, prices)
    7. Track execution time for each step and full batch

Tables Processed:
    - silver.crm_cust_info
    - silver.crm_prd_info
    - silver.crm_sales_details
    - silver.erp_cust_az12
    - silver.erp_loc_a101
    - silver.erp_px_cat_g1v2

Error Handling:
    - Uses TRY...CATCH block
    - Captures error message, line, state, and procedure

Notes:
    - This procedure performs a full reload (TRUNCATE + INSERT)
    - Intended for batch processing
    - Assumes source data is available in bronze layer

===============================================================
*/
CREATE or alter PROCEDURE silver.load_silver as
begin
begin try
    declare @start_time datetime, 
            @end_time datetime,
            @batch_start_time datetime,
            @batch_end_time datetime,
            @silver_load_start_time datetime,
            @silver_load_end_time datetime; 

    set @silver_load_start_time = getdate()
    print '---------------------------------------';
    print 'truncate then insert the CRM files';
    print '---------------------------------------';

    set @batch_start_time = getdate();
    set @start_time = getdate();

    print '>>Truncating silver.crm_cust_info';
    truncate table silver.crm_cust_info;

    PRINT '>> Inserting Data Into: silver.crm_cust_info';

    insert into silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lasttname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    select
        cst_id,
        cst_key,
        trim(cst_firstname) cst_firstname,
        trim(cst_lasttname) cst_lasttname,
        case 
            when upper(trim(cst_marital_status)) = 'S' then 'Single'
            when upper(trim(cst_marital_status)) = 'M' then 'Married'
            else  'n/a'
        end cst_marital_status,
        case 
            when upper(trim(cst_gndr)) = 'M' then 'Male'
            when upper(trim(cst_gndr)) = 'F' then 'Female'
            else  'n/a'
        end cst_gndr,
        cst_create_date
    from (
        select *,
               row_number() over (partition by cst_id order by cst_create_date) flag_number 
        from bronze.crm_cust_info
        where cst_id is not null
    ) t
    where flag_number = 1;

    set @end_time = getdate();

    print 'the time taken = ' + cast(datediff(second, @start_time, @end_time) as nvarchar(50)) + ' seconds';

    print '-------------------------------------';

    set @start_time = getdate();

    print '>>Truncating silver.crm_prd_info';
    truncate table silver.crm_prd_info;

    PRINT '>> Inserting Data Into: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
            AS DATE
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;

    set @end_time = getdate();

    print 'the time taken = ' + cast(datediff(second, @start_time, @end_time) as nvarchar(50)) + ' seconds';

    print '--------------------------------------------------';

    set @start_time = getdate();

    print '>>Truncating silver.crm_sales_details';
    truncate table silver.crm_sales_details;

    PRINT '>> Inserting Data Into: silver.crm_sales_details';

    insert into silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    select
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        case 
            when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
            else cast(cast(sls_order_dt as varchar(50)) as date) 
        end sls_order_dt,
        case 
            when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
            else cast(cast(sls_ship_dt as varchar(50)) as date) 
        end sls_ship_dt,
        case 
            when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
            else cast(cast(sls_due_dt as varchar(50)) as date) 
        end sls_due_dt,
        CASE 
            WHEN sls_sales IS NULL 
                 OR sls_sales <= 0 
                 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    from bronze.crm_sales_details;

    set @end_time = getdate();

    print 'the time taken = ' + cast(datediff(second, @start_time, @end_time) as nvarchar(50)) + ' seconds';
    
    set @batch_end_time = getdate();

    print 'the batch time taken = ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar(50)) + ' seconds';
    print '----------------------------------------------';

    set @batch_start_time = getdate();

    print '---------------------------------------';
    print 'truncate then insert the ERP files';
    print '---------------------------------------';

    set @start_time = getdate();

    print '>>Truncating silver.erp_cust_az12';
    truncate table silver.erp_cust_az12;

    PRINT '>> Inserting Data Into: silver.erp_cust_az12';

    insert into silver.erp_cust_az12
    (
        cid,
        bdate,
        gen
    )
    select  
        case 
            when cid like 'NAS%' then substring(cid, 4, len(cid))
            else cid
        end cid,
        case 
            when cast(bdate as date) > getdate() then null
            else cast(bdate as date)
        end bdate,
        case 
            when upper(trim(gen)) in ('M', 'Male') then 'Male'
            when upper(trim(gen)) in ('F', 'Female') then 'Female'
            else 'n/a'
        end gen
    from bronze.erp_cust_az12;

    set @end_time = getdate();

    print 'the time taken = ' + cast(datediff(second, @start_time, @end_time) as nvarchar(50)) + ' seconds';

    print '--------------------------------------------------';

    set @start_time = getdate();

    print '>>Truncating silver.erp_loc_a101';
    truncate table silver.erp_loc_a101;

    PRINT '>> Inserting Data Into: silver.erp_loc_a101';

    insert into silver.erp_loc_a101 (cid, cntry)
    select 
        replace(cid, '-', '') cid,
        case 
            when trim(cntry) = 'DE' then 'Germany'
            when trim(cntry) in ('US', 'USA') then 'United States'
            when trim(cntry) is null or trim(cntry) = '' then 'n/a'
            else trim(cntry)
        end cntry
    from bronze.erp_loc_a101;

    set @end_time = getdate();

    print 'the time taken = ' + cast(datediff(second, @start_time, @end_time) as nvarchar(50)) + ' seconds';

    print '--------------------------------------------------';

    set @start_time = getdate();

    print '>>Truncating silver.erp_px_cat_g1v2';
    truncate table silver.erp_px_cat_g1v2;

    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

    insert into silver.erp_px_cat_g1v2(
        id,
        cat,
        subcat,
        maintenance
    )
    select 
        id,
        cat,
        subcat,
        maintenance
    from bronze.erp_px_cat_g1v2;

    set @end_time = getdate();

    print 'the time taken = ' + cast(datediff(second, @start_time, @end_time) as nvarchar(50)) + ' seconds';

    set @batch_end_time = getdate();

    print 'the batch time taken = ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar(50)) + ' seconds';

    set @silver_load_end_time = getdate()
    print 'the Total time taken to load silver layer = ' + cast(datediff(second, @silver_load_start_time, @silver_load_end_time) as nvarchar(50)) + ' seconds';
End try
begin catch
    print' ERROR OCCURED '
    PRINT' The error Message is : ' + ERROR_MESSAGE();
    PRINT' THE ERROR LINE IS :' +CAST(ERROR_LINE() AS NVARCHAR(50));
    PRINT 'THE ERROR STATE IS :' + CAST(ERROR_STATE() AS NVARCHAR(50));
    PRINT 'THE ERROR PROCEDURE IS :' +ERROR_PROCEDURE()
END catch
end
