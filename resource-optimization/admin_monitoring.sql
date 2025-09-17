/************************************************************************************************************************************************************
Description:
Identifies all warehouses that do not have auto-resume enabled. Enabling this feature will automatically resume a warehouse any time a query is submitted against that specific warehouse. By default, all warehouses have auto-resume enabled.

How to Interpret Results:
Make sure all warehouses are set to auto resume. If you are going to implement auto suspend and proper timeout limits, this is a must or users will not be able to query the system.
************************************************************************************************************************************************************/
SHOW WAREHOUSES ->> 
    SELECT 
        "name" AS WAREHOUSE_NAME,
        "size" AS WAREHOUSE_SIZE
    FROM $1
    WHERE "auto_resume" = 'false'
;

/************************************************************************************************************************************************************
Description:
Identifies all warehouses that do not have auto-suspend enabled. Enabling this feature will ensure that warehouses become suspended after a specific amount of inactive time in order to prevent runaway costs. By default, all warehouses have auto-suspend enabled.

How to Interpret Results:
Make sure all warehouses are set to auto suspend. This way when they are not processing queries your compute footprint will shrink and thus your credit burn.
************************************************************************************************************************************************************/
SHOW WAREHOUSES ->>
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
  FROM $1
 WHERE IFNULL("auto_suspend",0) = 0
;

/************************************************************************************************************************************************************
Description:
Identifies warehouses that have the longest setting for automatic suspension after a period of no activity on that warehouse.

How to Interpret Results:
All warehouses should have an appropriate setting for automatic suspension for the workload.

– For Tasks, Loading and ETL/ELT warehouses set to immediate suspension.
– For BI and SELECT query warehouses set to 10 minutes for suspension to keep data caches warm for end users
– For DevOps, DataOps and Data Science warehouses set to 5 minutes for suspension as warm cache is not as important to ad-hoc and highly unique queries.
************************************************************************************************************************************************************/
SHOW WAREHOUSES ->>
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
      , "auto_suspend" AS AUTO_SUSPEND_SEC
      , ROUND("auto_suspend"/60,0) AS AUTO_SUSPEND_MIN
  FROM $1
 --WHERE "auto_suspend" >= 3600  // 3600 seconds = 1 hour
ORDER BY AUTO_SUSPEND_SEC DESC;

/************************************************************************************************************************************************************
Description:
Identifies all warehouses without resource monitors in place. Resource monitors provide the ability to set limits on credits consumed against a warehouse during a specific time interval or date range. This can help prevent certain warehouses from unintentionally consuming more credits than typically expected.

How to Interpret Results:
Warehouses without resource monitors in place could be prone to excessive costs if a warehouse consumes more credits than anticipated. Leverage the results of this query to identify the warehouses that should have resource monitors in place to prevent future runaway costs.
************************************************************************************************************************************************************/
SHOW WAREHOUSES ->>
SELECT "name" AS WAREHOUSE_NAME
      ,"size" AS WAREHOUSE_SIZE
      ,"resource_monitor" AS RESOURCE_MONITOR_NAME
FROM $1
WHERE "resource_monitor" = 'null'
;

/************************************************************************************************************************************************************
Description:
Lists out all warehouses that are used by multiple ROLEs in Snowflake and returns the average execution time and count of all queries executed by each ROLE in each warehouse.

How to Interpret Results:
If execution times or query counts across roles within a single warehouse are wildly different it might be worth segmenting those users into separate warehouses and configuring each warehouse to meet the specific needs of each workload

Primary Schema:
Account_Usage
************************************************************************************************************************************************************/
SELECT *
FROM (
    SELECT 
        WAREHOUSE_NAME
        ,ROLE_NAME
        ,AVG(EXECUTION_TIME) as AVERAGE_EXECUTION_TIME
        ,COUNT(QUERY_ID) as COUNT_OF_QUERIES
        ,COUNT(ROLE_NAME) OVER(PARTITION BY WAREHOUSE_NAME) AS ROLES_PER_WAREHOUSE
    FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
    where to_date(start_time) >= dateadd(month,-1,CURRENT_TIMESTAMP())
    group by 1,2
    ) A
WHERE A.ROLES_PER_WAREHOUSE > 1
order by 5 DESC,1,2
;

/************************************************************************************************************************************************************
8. Idle Users (T2)
Tier 2
Description:
Users in the Snowflake platform that have not logged in in the last 30 days

How to Interpret Results:
Should these users be removed or more formally onboarded?

Primary Schema:
Account_Usage
************************************************************************************************************************************************************/
SELECT 
	*
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS 
WHERE LAST_SUCCESS_LOGIN < DATEADD(month, -1, CURRENT_TIMESTAMP()) 
AND DELETED_ON IS NULL;

/************************************************************************************************************************************************************
9. Users Never Logged In (T2)
Tier 2
Description:
Users that have never logged in to Snowflake

How to Interpret Results:
Should these users be removed or more formally onboarded?

Primary Schema:
Account_Usage
************************************************************************************************************************************************************/
SELECT 
	*
FROM SNOWFLAKE.ACCOUNT_USAGE.USERS 
WHERE LAST_SUCCESS_LOGIN IS NULL;

/************************************************************************************************************************************************************
10. Idle Roles (T2)
Tier 2
Description:
Roles that have not been used in the last 30 days

How to Interpret Results:
Are these roles necessary? Should these roles be cleaned up?

Primary Schema:
Account_Usage
************************************************************************************************************************************************************/
SELECT 
	R.*
FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES R
LEFT JOIN (
    SELECT DISTINCT 
        ROLE_NAME 
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
    WHERE START_TIME > DATEADD(month,-1,CURRENT_TIMESTAMP())
    ) Q 
    ON Q.ROLE_NAME = R.NAME
WHERE Q.ROLE_NAME IS NULL
and DELETED_ON IS NULL;

/************************************************************************************************************************************************************
11. Idle Warehouses (T2)
Tier 2
Description:
Warehouses that have not been used in the last 30 days

How to Interpret Results:
Should these warehouses be removed? Should the users of these warehouses be enabled/onboarded?
************************************************************************************************************************************************************/
SHOW WAREHOUSES ->>
select 
    * 
from $1 a
left join (
    select 
        distinct WAREHOUSE_NAME 
    from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
    WHERE START_TIME > DATEADD(month,-1,CURRENT_TIMESTAMP())
    ) b 
    on b.WAREHOUSE_NAME = a."name"
where b.WAREHOUSE_NAME is null
;

/************************************************************************************************************************************************************
12. Set Statement Timeouts (T2)
Tier 2
Description:
Statement timeouts provide additional controls around how long a query is able to run before cancelling it. Using this feature will ensure that any queries that get hung up for extended periods of time will not cause excessive consumption of credits.

Show parameter settings at the Account, Warehouse, and User Session levels.

How to Interpret Results:
This parameter is set at the account level by default. When the parameter is also set for both a warehouse and a user session, the lowest non-zero value is enforced.
************************************************************************************************************************************************************/
SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS' IN ACCOUNT;
SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS' IN WAREHOUSE <warehouse-name>;
SHOW PARAMETERS LIKE 'STATEMENT_TIMEOUT_IN_SECONDS' IN USER <username>;

/************************************************************************************************************************************************************
13. Stale Table Streams (T2)
Tier 2
Description:
Indicates whether the offset for the stream is positioned at a point earlier than the data retention period for the table (or 14 days, whichever period is longer). Change data capture (CDC) activity cannot be returned for the table.

How to Interpret Results:
To return CDC activity for the table, recreate the stream. To prevent a stream from becoming stale, consume the stream records within a transaction during the retention period for the table.
************************************************************************************************************************************************************/
SHOW STREAMS ->>
select * 
from $1
where "stale" = true;

/************************************************************************************************************************************************************
14. Failed Tasks (T2)
Tier 2
Description:
Returns a list of task executions that failed.

How to Interpret Results:
Revisit these task executions to resolve the errors.

Primary Schema:
Account_Usage
************************************************************************************************************************************************************/
select *
from snowflake.account_usage.task_history
WHERE STATE = 'FAILED'
and query_start_time >= DATEADD (day, -7, CURRENT_TIMESTAMP())
order by query_start_time DESC
;

/************************************************************************************************************************************************************
15. Long Running Tasks (T2)
Tier 2
Description:
Returns an ordered list of the longest running tasks

How to Interpret Results:
revisit task execution frequency or the task code for optimization

Primary Schema:
Account_Usage
************************************************************************************************************************************************************/
select 
    DATEDIFF(seconds, QUERY_START_TIME,COMPLETED_TIME) as DURATION_SECONDS
    ,*
from snowflake.account_usage.task_history
WHERE STATE = 'SUCCEEDED'
and query_start_time >= DATEADD (day, -7, CURRENT_TIMESTAMP())
order by DURATION_SECONDS desc
;