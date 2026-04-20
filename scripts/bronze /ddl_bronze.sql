/*
===============================================================
Create Bronze Layer Tables (CRM & ERP Data)
===============================================================

Script Purpose:
    This script prepares the 'bronze' layer in the DataWarehouse by 
    dropping existing tables (if they exist) and recreating them.

    The bronze layer represents the raw data ingestion stage, where 
    data is loaded from source systems without transformation.

    The script creates the following tables:
        - CRM Tables:
            'crm_cust_info'      → Customer information
            'crm_prd_info'       → Product information
            'crm_sales_details'  → Sales transactions

        - ERP Tables:
            'erb_PX_CAT_G1V2'    → Product categories
            'erb_CUST_AZ12'      → Customer demographic data
            'erb_LOC_A101'       → Customer location data

    These tables will be used as the initial step in the ETL process,
    before cleaning and transforming data into the silver and gold layers.

WARNING:
    Running this script will drop existing tables in the 'bronze' schema.
    All data stored in these tables will be permanently deleted.

    Ensure that you have proper backups before executing this script,
    especially in production environments.
*/

if object_id ('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lasttname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);
GO
if object_id ('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);
GO
if object_id ('bronze.crm_sales_details','U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);
go

if object_id ('bronze.erb_px_cat_g1v2','U') IS NOT NULL
	DROP TABLE bronze.erb_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
);

go 

if object_id ('bronze.erp_cust_az12','U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
CID NVARCHAR(50),
BDATE Datetime,
GEN NVARCHAR(50)
);
go

if object_id ('bronze.erp_loc_a101','U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);
