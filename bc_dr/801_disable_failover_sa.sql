--Cleanup: Disable failover in source account.

use role accountadmin;
alter connection crosscontinent disable failover to accounts SFPSCOGS.PPARASHAR_AZURE_WESTEU_BC;
drop connection crosscontinent;
drop failover group sales_payroll_failover;