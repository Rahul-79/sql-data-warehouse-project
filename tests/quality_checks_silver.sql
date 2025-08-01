-- 1.CHECK FOR NULL AND DUPLICATE VALUES 
-- EXPECTATION: NO RESULTS

-- customer table
select cst_id,count(*) c
from bronze.crm_cust_info
group by cst_id 
having count(*)>1 or cst_id is null

-- product table
select prd_id,count(*) c
from bronze.crm_prd_info
group by prd_id 
having count(*)>1 or prd_id is null


-- 2.CHECK FOR UNWANTED SPACES 
-- EXPECTATION: NO RESULTS

-- customer table
select * from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

-- product table
select * from bronze.crm_prd_info
where prd_nm != trim(prd_nm)



-- 3. DATA STANDARDIZATION AND CONSISTENCY

-- customer table
select distinct(cst_gndr)
from bronze.crm_cust_info

select distinct(cst_maritial_status)
from bronze.crm_cust_info

-- product table
select distinct(prd_line) 
from bronze.crm_prd_info

-- erp_cust_az12 table
select distinct(gen),
case 
    when upper(replace(replace(gen,char(10),''),char(13),'')) in ('F','Female') then 'Female'
    when upper(replace(replace(gen,char(10),''),char(13),'')) in ('M','Male') then 'Male'
    else 'Unknown'
end gen
from 
bronze.erp_cust_az12

-- erp_loc_a101 table
select distinct(cntry),
case 
    when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) in ('DE','Germany') then 'Germany'
    when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) in ('US','USA','UNITED STATES') then 'United States'
    when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) = 'Canada' then 'Canada'
    when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) = 'Australia' then 'Australia'
    when upper(trim(replace(replace(cntry,char(10),''),char(13),''))) = 'France' then 'France'
    else 'Unknown'
end cntry
from bronze.erp_loc_a101



-- 4. CHECK FOR NULLS AND NEGATIVE NUMBERS

-- product table
select prd_cost from 
bronze.crm_prd_info 
where prd_cost < 0 or prd_cost is null



-- 5. CHECK FOR INVALID DATE ORDERS

-- product table
select * from bronze.crm_prd_info
where prd_start_date>prd_end_date

-- sales details table
select 
nullif(sls_order_dt,0)
from bronze.crm_sales_details
where sls_order_dt <=0 
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101 

select * from 
bronze.crm_sales_details
where sls_order_dt>sls_ship_dt
or sls_order_dt>sls_due_dt


-- 6. CONSISTENCY BETWEEN SALES,PRICES AND QUANTITY

-- sales details table
select
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
where sls_sales != sls_quantity * sls_price or
sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales < 0 or sls_quantity < 0 or sls_price < 0
order by sls_sales,sls_quantity

-- 6. SELECT OUT OF RANGE DATES

-- erp_cust_az12 table
select distinct(bdate)
from bronze.erp_cust_az12
where bdate<'1924-01-01' or bdate>getdate()
