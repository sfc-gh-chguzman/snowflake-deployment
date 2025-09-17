--Cleanup: Disable failover in target account.

use role accountadmin;
drop failover group sales_payroll_failover;
drop connection crosscontinent;