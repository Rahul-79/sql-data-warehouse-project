create or alter procedure silver.load_silver as
begin
    truncate table silver.crm_cust_info
    insert into silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_maritial_status,cst_gndr,cst_create_date)
    select 
    cst_id,
    cst_key,
    trim(cst_firstname) cst_firstname,
    trim(cst_lastname) cst_lastname,
    case
        when upper(trim(cst_maritial_status))='M' then 'Married'
        when upper(trim(cst_maritial_status))='S' then 'Single'
        else 'Unknown'
    end cst_maritial_status, 
    case
        when upper(trim(cst_gndr))='M' then 'Male'
        when upper(trim(cst_gndr))='F' then 'Female'
        else 'Unknown'
    end cst_gndr,
    cst_create_date from (
    select * from (
    select *,
    row_number() over (partition by cst_id order by cst_create_date desc) flag
    from bronze.crm_cust_info where cst_id is not null)t 
    where t.flag=1)t1

    truncate table silver.crm_prd_info
    if object_id('silver.crm_prd_info','u') is not null 
        drop table silver.crm_prd_info
    create table silver.crm_prd_info(
        prd_id int,
        cat_id nvarchar(50),
        prd_key nvarchar(50),
        prd_nm nvarchar(50),
        prd_cost int,
        prd_line nvarchar(50),
        prd_start_date date,
        prd_end_date date,
        dwh_create_date datetime2 default getdate()
    );


    insert into silver.crm_prd_info(prd_id, cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_date,prd_end_date)
    select prd_id,
    replace(substring(prd_key,1,5),'-','_') as cat_id,
    substring(prd_key,7,len(prd_key)) as prd_key,
    prd_nm,
    isnull(prd_cost,0) prd_cost,
    case upper(trim(prd_line)) 
        when 'M' then 'Mountain'
        when 'R' then 'Road'
        when 'S' then 'Other Sales'
        when 'T' then 'Touring'
        else 'Unkown'
    end as prd_line,
    cast(prd_start_date as date) prd_start_date,
    cast(lead(prd_start_date) over (partition by prd_key order by prd_start_date)-1 as date) prd_end_date
    from bronze.crm_prd_info

    truncate table silver.crm_sales_details
    if object_id('silver.crm_sales_details','u') is not null 
        drop table silver.crm_sales_details

    create table silver.crm_sales_details(
        sls_ord_num nvarchar(50),
        sls_prd_key nvarchar(50),
        sls_cust_id int,
        sls_order_dt date,
        sls_ship_dt date,
        sls_due_dt date,
        sls_sales int,
        sls_quantity int,
        sls_price int,
        dwh_create_date datetime2 default getdate()
    )


    insert into  silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales, sls_quantity,sls_price)
    select 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case 
        when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
        else cast(cast(sls_order_dt as varchar) as date)
    end sls_order_dt,
    case 
        when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
        else cast(cast(sls_ship_dt as varchar) as date)
    end sls_ship_dt,
    case 
        when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
        else cast(cast(sls_due_dt as varchar) as date)
    end sls_due_dt,
    case 
        when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
            then sls_quantity * abs(sls_price)
        else sls_sales
    end sls_sales,
    sls_quantity,
    case 
        when sls_price is null or sls_price<=0 
            then sls_sales/nullif(sls_quantity,0)
        else sls_price
    end sls_price
    from bronze.crm_sales_details
    

    truncate table silver.erp_cust_az12
    insert into silver.erp_cust_az12(cid,bdate,gen)
    select
    case 
        when cid like 'NAS%' then substring(cid,4,len(cid))
        else cid
    end cid,
    case 
        when bdate>getdate() then null
        else bdate
    end bdate,
    case  
        when upper(trim(replace(replace(gen,char(10),''),char(13),''))) in ('F','Female') then 'Female'
        when upper(trim(replace(replace(gen,char(10),''),char(13),''))) in ('M','Male') then 'Male'
        else 'Unknown'
    end gen
    from 
    bronze.erp_cust_az12

    truncate table silver.erp_loc_a101
    insert into silver.erp_loc_a101(cid,cntry)
    select 
    replace(cid,'-','') cid,
    case 
        when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) in ('DE','Germany') then 'Germany'
        when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) in ('US','USA','UNITED STATES') then 'United States'
        when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) = 'Canada' then 'Canada'
        when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) = 'Australia' then 'Australia'
        when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) = 'France' then 'France'
        else 'Unknown'
    end cntry
    from bronze.erp_loc_a101


    truncate table silver.erp_px_cat_g1v2
    insert into silver.erp_px_cat_g1v2
    (id,cat,subcat,maintenance)
    select id,
    cat,
    subcat,
    case 
        when replace(replace(maintenance,char(10),''),char(13),'') = 'Yes' then 'Yes'
        else 'No'
    end maintenance
    from bronze.erp_px_cat_g1v2 
end
