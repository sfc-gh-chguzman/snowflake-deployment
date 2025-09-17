--Create tables and data share and ingests data in tables.

use role sysadmin;
create table if not exists payroll.noam_northeast.employee_detail (
   first_name varchar
   ,last_name varchar
   ,email varchar
   ,gender varchar
   ,ssn varchar
   ,street varchar
   ,city varchar
   ,state varchar
   ,postcode varchar
   ,age varchar
   ,birthday date
   ,iban varchar
   ,card_type varchar
   ,cc varchar
   ,ccexp varchar
   ,occupation varchar
   ,salary number
   ,education varchar
   ,credit_score_provider varchar
   ,credit_score number
   ,company varchar
);
use schema payroll.noam_northeast;
use warehouse etl_wh;
put file://<localtion_of_hr_data.csv> @%employee_detail;
create file format if not exists csv_standard
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = true
    compression = gzip;
copy into employee_detail
    from @%employee_detail
    file_format = (format_name = csv_standard);
alter warehouse etl_wh set warehouse_size='X-Large';
create table if not exists global_sales.noam_online_retail.customer as select * from snowflake_sample_data.tpch_sf1.customer; 
create table if not exists global_sales.noam_online_retail.lineitem as select * from snowflake_sample_data.tpch_sf1.lineitem;
create table if not exists global_sales.noam_online_retail.nation as select * from snowflake_sample_data.tpch_sf1.nation;
create table if not exists global_sales.noam_online_retail.orders as select * from snowflake_sample_data.tpch_sf1.orders;
create table if not exists global_sales.noam_online_retail.part as select * from snowflake_sample_data.tpch_sf1.part;
create table if not exists global_sales.noam_online_retail.partsupp as select * from snowflake_sample_data.tpch_sf1.partsupp;
create table if not exists global_sales.noam_online_retail.region as select * from snowflake_sample_data.tpch_sf1.region;
create table if not exists global_sales.noam_online_retail.supplier as select * from snowflake_sample_data.tpch_sf1.supplier;
update global_sales.noam_online_retail.orders set o_orderdate=dateadd(day,-1,current_date()) 
    where o_orderdate=(select max(o_orderdate) from global_sales.noam_online_retail.orders);
alter warehouse etl_wh set warehouse_size='X-Small';
create table if not exists common.utility.mkt_segment_mapping (
      sales_role varchar(30),
      market_segment varchar(30)
);
insert overwrite into common.utility.mkt_segment_mapping values 
    ('SALES_ANALYST','AUTOMOBILE'),
    ('SALES_ANALYST','MACHINERY'),
    ('SALES_ADMIN','BUILDING'),
    ('SALES_ADMIN','HOUSEHOLD'),
    ('SALES_ADMIN','AUTOMOBILE'),
    ('SALES_ADMIN','MACHINERY');
use role accountadmin;
create share global_sales_share;
grant usage on database global_sales to share global_sales_share;
grant usage on schema global_sales.noam_online_retail to share global_sales_share;
grant select on table global_sales.noam_online_retail.customer to share global_sales_share;
grant select on table global_sales.noam_online_retail.lineitem to share global_sales_share;
grant select on table global_sales.noam_online_retail.nation to share global_sales_share;