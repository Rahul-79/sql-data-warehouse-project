create or alter procedure bronze.load_bronze as
begin 
    declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime
    begin try 
        set @batch_start_time = getdate()
        print 'loading bronze layer'
        print '==================================================='
        print 'loading crm tables'
        set @start_time = getdate();
        truncate table bronze.crm_cust_info;
        bulk insert bronze.crm_cust_info
        from '/var/opt/mssql/cust_info.csv'
        with (
            firstrow = 2,
            fieldterminator  = ',',
            tablock
        );
        set @end_time = getdate();
        print 'load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';

        set @start_time = getdate();
        truncate table bronze.crm_prd_info;
        bulk insert bronze.crm_prd_info
        from '/var/opt/mssql/prd_info.csv'
        with (
            firstrow = 2,
            fieldterminator  = ',',
            tablock
        );
        set @end_time = getdate();
        print 'load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';

        set @start_time = getdate();
        truncate table bronze.crm_sales_details;
        bulk insert bronze.crm_sales_details
        from '/var/opt/mssql/sales_details.csv'
        with (
            firstrow = 2,
            fieldterminator  = ',',
            tablock
        );
        set @end_time = getdate();
        print 'load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
        
        print '==================================================='
        print 'loading crp tables'
        set @start_time = getdate();
        truncate table bronze.erp_loc_a101;
        bulk insert bronze.erp_loc_a101
        from '/var/opt/mssql/loc_a101.csv'
        with (
            firstrow = 2,
            fieldterminator = ',',
            tablock
        );
        set @end_time = getdate();
        print 'load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';

        set @start_time = getdate();
        truncate table bronze.erp_cust_az12;
        bulk insert bronze.erp_cust_az12
        from '/var/opt/mssql/cust_az12.csv'
        with (
            firstrow = 2,
            fieldterminator  = ',',
            tablock
        );
        set @end_time = getdate();
        print 'load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';

        set @start_time = getdate();
        truncate table bronze.erp_px_cat_g1v2;
        bulk insert bronze.erp_px_cat_g1v2
        from '/var/opt/mssql/px_cat_g1v2.csv'
        with (
            firstrow = 2,
            fieldterminator  = ',',
            tablock
        );
        set @end_time = getdate();
        print 'load duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
        print '=================================================================================='
        set @batch_end_time = getdate()
        print 'total batch duration '+ cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'seconds'
    end try 
    begin catch 
        print '=================================================================================='
        print 'error occurred during loading bronze layer'
        print 'error message' + error_message()
        print 'error message' + cast(error_number() as nvarchar)
    end catch 
end
