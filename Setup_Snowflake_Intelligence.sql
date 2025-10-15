USE ROLE ACCOUNTADMIN;

/*
    0.  Pre-Req (Optional)
*/

--Creating a Role for an Admin to be able to create Agents
CREATE ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

--Granting SNOWFLAKE_INTELLIGENCE_ADMIN to Admin User 
GRANT ROLE SNOWFLAKE_INTELLIGENCE_ADMIN TO USER CHGUZMAN; --Provide to user or functional role. Using myself for purpose of this demo. 

/*
    1.  Create a database. This holds the configuration object and the other objects used to support Snowflake Intelligence.
*/

CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
GRANT USAGE ON DATABASE snowflake_intelligence TO ROLE PUBLIC;

/*
    2.  After you set up the snowflake_intelligence database, use the following SQL commands to create a schema to store the agents and make them discoverable to
        everyone.
*/

CREATE SCHEMA IF NOT EXISTS snowflake_intelligence.agents;
GRANT USAGE ON SCHEMA snowflake_intelligence.agents TO ROLE PUBLIC;

/*
    3.  Grant the CREATE AGENT privilege on the agents schema to any role that should be able to create agents for Snowflake Intelligence.
        --GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE <role>;
*/

GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE SNOWFLAKE_INTELLIGENCE_ADMIN;

/*
    4.  Search for "Snowflake Documentation" on Marketplace. Get the Data Share which contains the Search Service. 
        https://app.snowflake.com/marketplace/listing/GZSTZ67BY9OQ4/snowflake-snowflake-documentation
        Add the Role that can access this shared database (e.g. SNOWFLAKE_INTELLIGENCE_ADMIN, PUBLIC)

        Once Marketplace data is retrieved, you should see it under SNOWFLAKE_DOCUMENTATION.SHARED schema
*/

DESCRIBE CORTEX SEARCH SERVICE SNOWFLAKE_DOCUMENTATION.SHARED.CKE_SNOWFLAKE_DOCS_SERVICE; 
--GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_DOCUMENTATION TO ROLE PUBLIC;
