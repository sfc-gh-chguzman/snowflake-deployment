--Creates connection and failover group.

use role accountadmin;
create connection crosscontinent;
alter connection crosscontinent enable failover to accounts SFPSCOGS.PPARASHAR_AZURE_WESTEU_BC;
create failover group sales_payroll_failover
    object_types = users, roles, warehouses, resource monitors, databases, shares
    allowed_databases = global_sales,common,payroll,inventory,loyalty
    allowed_shares = global_sales_share
    allowed_accounts = <org_name.account_name>
--  replication_schedule = '180 MINUTES';