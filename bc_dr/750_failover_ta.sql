--Promotes the secondary connection and failover group to primary.

use role accountadmin;
alter connection crosscontinent primary;
alter failover group sales_payroll_failover primary;