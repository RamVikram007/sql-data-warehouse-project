/* ==============================================================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
=================================================================================================
Script Purpose:
  This Stored Procedure loads data into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' command top load data from CSV files to bronze tables.

Parameters:
   None.
This Stored Procedure does not accept any parameters or return fany values.

Usage Example:
    EXEC bronze.load_bronze;
================================================================================================*/

create or alter procedure bronze.load_bronze as
begin
	declare @starttime datetime, @endtime datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		print '=========================================================';
		print 'Loading Bronze layer';
		print '=========================================================';

		print '---------------------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------------------';

		set @starttime = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\sql\dwh_project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.crm_cust_info;
		set @endtime = GETDATE();
		print '>> Load Duration: ' + Cast(datediff(second, @starttime, @endtime) as nvarchar) + ' seconds';
		print '>> ------------';
		
		set @starttime = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\sql\dwh_project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.crm_prd_info;
		set @endtime = GETDATE();
		print '>> Load Duration: ' + Cast(datediff(second, @starttime, @endtime) as nvarchar) + ' seconds';
		print '>> ------------';

		set @starttime = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\sql\dwh_project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.crm_sales_details;
		set @endtime = GETDATE();
		print '>> Load Duration: ' + Cast(datediff(second, @starttime, @endtime) as nvarchar) + ' seconds';
		print '>> ------------';

		print '---------------------------------------------------------';
		print 'Loading ERP Tables';
		print '---------------------------------------------------------';

		set @starttime = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		print '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\sql\dwh_project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.erp_cust_az12;
		set @endtime = GETDATE();
		print '>> Load Duration: ' + Cast(datediff(second, @starttime, @endtime) as nvarchar) + ' seconds';
		print '>> ------------';

		set @starttime = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		print '>> Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\sql\dwh_project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.erp_loc_a101;
		set @endtime = GETDATE();
		print '>> Load Duration: ' + Cast(datediff(second, @starttime, @endtime) as nvarchar) + ' seconds';
		print '>> ------------';

		set @starttime = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\sql\dwh_project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		select count(*) from bronze.erp_px_cat_g1v2;
		set @endtime = GETDATE();
		print '>> Load Duration: ' + Cast(datediff(second, @starttime, @endtime) as nvarchar) + ' seconds';
		print '>> ------------';

		set @batch_end_time = GETDATE();
		print '==============================================';
		print 'Loading Bronze Layer is completed.';
		print '	  - Total Load Duration: ' + Cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
		print '===============================================';
	end try
	begin catch
		print '===============================================';
		print 'Error Occured During Loading Bronze Layer';
		print 'Error Message' + Error_message();
		print 'Error Message' + Cast(Error_number() as nvarchar);
		print 'Error Message' + Cast(Error_state() as nvarchar);
		print '===============================================';
	end catch
end;
