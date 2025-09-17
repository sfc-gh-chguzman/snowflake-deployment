--Creates replica of connection and failover group in the target account.

use role accountadmin;
create connection CROSSCONTINENT
    as replica of SFPSCOGS.PPARASHAR_US_WEST_BC.CROSSCONTINENT;
create failover group SALES_PAYROLL_FAILOVER
    as replica of SFPSCOGS.PPARASHAR_US_WEST_BC.SALES_PAYROLL_FAILOVER;