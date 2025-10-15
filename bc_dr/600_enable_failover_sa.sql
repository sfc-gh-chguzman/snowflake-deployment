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


ALTER FAILOVER GROUP sales_payroll_failover SET allowed_databases = global_sales,common,payroll,inventory,loyalty,COMMON_UTILITY,DEMO_DB,SECURITY_NETWORK_DB;
ALTER FAILOVER GROUP sales_payroll_failover SET object_types = users, roles, warehouses, resource monitors, databases, shares, account parameters, NETWORK POLICIES
;
ALTER FAILOVER GROUP sales_payroll_failover SET allowed_accounts = SFPSCOGS.CHGUZMAN_AZURE_DEMO;

SHOW FAILOVER GROUPS;

ALTER FAILOVER GROUP ADD 
    NETWORK POLICY 'ACCOUNT_VPN_POLICY_SE';

  SELECT * 
    FROM TABLE(
      INFORMATION_SCHEMA.REPLICATION_GROUP_DANGLING_REFERENCES('sales_payroll_failover')
  );

SHOW FAILOVER GROUPS;
SHOW DATABASES IN FAILOVER GROUP sales_payroll_failover;
SHOW SHARES IN FAILOVER GROUP sales_payroll_failover;

SHOW REPLICATION ACCOUNTS ->> SELECT * FROM $1 ;

SHOW USERS;
DROP USER CHGUZMAN_1;

ALTER USER CHGUZMAN UNSET SESSION POLICY;

ALTER FAILOVER GROUP sales_payroll_failover
  ADD POLICY SECURITY_NETWORK_DB.POLICIES.ACCOUNT_MFA_AUTH_POLICY;


DESCRIBE NETWORK POLICY SECURITY_NETWORK_DB.POLICIES.ACCOUNT_MFA_AUTH_POLICY;
SHOW NETWORK POLICIES;

SHOW PARAMETERS IN USER CHGUZMAN ->> SELECT * FROM $1 WHERE "key" LIKE '%NETWORK%';

SHOW failover groups;