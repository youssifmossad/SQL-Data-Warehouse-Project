/*
===============================================================
Load Bronze Layer (Summary)
===============================================================

This stored procedure loads raw data into the bronze layer by:
- Truncating existing tables
- Importing data from CRM and ERP CSV files using BULK INSERT
- Measuring load time for each table and batch

Includes error handling using TRY...CATCH to display error details.

WARNING:
All bronze tables are truncated before loading, so existing data will be lost.

Usage:
EXEC bronze.load_bronze;
*/
create or alter procedure bronze.load_bronze as 
begin
    begin Try  
    declare @start_time Datetime ,@end_time datetime,@batch_start_time datetime ,@batch_end_time datetime;
    print '==============================='
    print 'loading the bronze layer'
    print '==============================='
    print'==============================='
    print 'loading CRM tables'
    print'==============================='
    set @batch_start_time=getdate();
    set @start_time=getdate();

    print '...truncate table : bronze.crm_cust_info'
    truncate table bronze.crm_cust_info

    print 'Insert data into : bronze.crm_cust_info'
    Bulk insert bronze.crm_cust_info
    from 'C:\Users\HP\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
    with (
         firstrow =2,
         fieldterminator=',',
         tablock
         );
    set @end_time=getdate();

    print 'load duration'+cast(datediff(second,@start_time,@end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'
    ----------------------
    set @start_time=getdate();
    print '...truncate table : bronze.crm_cust_info'
    truncate table bronze.crm_prd_info

    print 'Insert data into : bronze.crm_prd_info'
    Bulk insert bronze.crm_prd_info
    from 'C:\Users\HP\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
    with (
         firstrow =2,
         fieldterminator=',',
         tablock
         );

    set @end_time=getdate();

    print 'load duration'+cast(datediff(second,@start_time,@end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'
    -----------------------
    set @start_time=getdate();
    print '...truncate table : bronze.crm_cust_info'
    truncate table bronze.crm_sales_details

    print 'Insert data into : bronze.crm_sales_details'
    Bulk insert bronze.crm_sales_details
    from 'C:\Users\HP\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
    with (
         firstrow =2,
         fieldterminator=',',
         tablock
         );
    print 'load duration'+cast(datediff(second,@start_time,@end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'

    set @batch_end_time = getdate();
    print '------------------------------------------------------'
    print ' total crm batch load time : ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'
    -----------------------
    set @batch_start_time=getdate();
    set @start_time=getdate();
    print '...truncate table : bronze.crm_cust_info'
    truncate table bronze.erp_PX_CAT_G1V2

    print 'Insert data into : bronze.erp_PX_CAT_G1V2'
    Bulk insert bronze.erp_PX_CAT_G1V2
    from 'C:\Users\HP\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
    with (
         firstrow =2,
         fieldterminator=',',
         tablock
         );
    print 'load duration'+cast(datediff(second,@start_time,@end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'
    -----------------------
    set @start_time=getdate();
    print '...truncate table : bronze.crm_cust_info'
    truncate table bronze.erp_cust_az12

    print 'Insert data into : bronze.erp_cust_az12'
    Bulk insert bronze.erp_cust_az12
    from 'C:\Users\HP\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
    with (
         firstrow =2,
         fieldterminator=',',
         tablock
         );
    print 'load duration'+cast(datediff(second,@start_time,@end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'
    ------------------------
    set @start_time=getdate();
    print '...truncate table : bronze.crm_cust_info'
    truncate table bronze.erp_loc_a101

    print 'Insert data into : bronze.erp_loc_a101'
    Bulk insert bronze.erp_loc_a101
    from 'C:\Users\HP\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
    with (
         firstrow =2,
         fieldterminator=',',
         tablock
         );
    print 'load duration'+cast(datediff(second,@start_time,@end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------';

    set @batch_end_time = getdate();

    print '------------------------------------------------------'
    print ' total erp batch load time : ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar(50))+' seconds ';
    print'-------------------------------------------------------'

    End try 
    BEGIN CATCH
        PRINT '❌ Error occurred';

        PRINT 'Message: ' + ERROR_MESSAGE();
        PRINT 'Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
        PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
end

exec bronze.load_bronze
