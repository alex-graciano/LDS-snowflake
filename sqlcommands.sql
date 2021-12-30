CREATE OR REPLACE api integration api_integration_lds
    api_provider = aws_api_gateway
    api_aws_role_arn = 'arn:aws:iam::*******:role/lds-snowflake'
    enabled = true
    api_allowed_prefixes = ('https://******.execute-api.eu-west-3.amazonaws.com/dev/geocode-table');
    
    
DESCRIBE integration api_integration_lds;


CREATE OR REPLACE EXTERNAL FUNCTION geocode_table
(cloud VARCHAR, toke VARCHAR, addresses_array VARIANT)
RETURNS VARIANT
api_integration = api_integration_lds
AS 'https://*****.execute-api.eu-west-3.amazonaws.com/dev/geocode-table'

CREATE OR REPLACE PROCEDURE CARTO_BACKEND_DATA_TEAM.AGRACIANO_TEST.GEOCODE_TABLE_PROC
(INPUT VARCHAR, OUTPUT_TABLE VARCHAR, GEOCODE_COLUMN VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
    $$
    
    // Table setup
    var sqlCommand = "CREATE OR REPLACE TABLE " + OUTPUT_TABLE + " AS SELECT *, TO_GEOGRAPHY(null) AS geom FROM (" + INPUT + ") LIMIT 0";
    
    snowflake.execute (
        {sqlText: sqlCommand}
    );
    
    // Generate the array for batching
    sqlCommand = "SELECT ARRAY_AGG(" + GEOCODE_COLUMN + ") FROM ( "+ INPUT +")";
    
    var addresses = snowflake.execute (
        {sqlText: sqlCommand}
    );
    
    addresses.next();
    var addressesArray = addresses.getColumnValue(1);
    var quotedAndCommaSeparated = "'" + addressesArray.join("','") + "'";
    
    // Call to the Geocoding API
    sqlCommand = "SELECT carto_backend_data_team.public.geocode_table('snowflake', 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImRVNGNZTHAwaThjYnVMNkd0LTE0diJ9.eyJodHRwOi8vYXBwLmNhcnRvLmNvbS9lbWFpbCI6ImFncmFjaWFub0BjYXJ0b2RiLmNvbSIsImh0dHA6Ly9hcHAuY2FydG8uY29tL2FjY291bnRfaWQiOiJhY183eGhmd3ltbCIsImlzcyI6Imh0dHBzOi8vYXV0aC5jYXJ0by5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDExNjMwMjExMjE4NDQ0MTI3NjQiLCJhdWQiOiJjYXJ0by1jbG91ZC1uYXRpdmUtYXBpIiwiaWF0IjoxNjQwODU1OTkzLCJleHAiOjE2NDA5NDIzOTMsImF6cCI6ImpDV25ISzZFMksyYU95OWpMeTNPN1pNcGhxR085QlBMIiwic2NvcGUiOiJyZWFkOmN1cnJlbnRfdXNlciIsInBlcm1pc3Npb25zIjpbInJlYWQ6YWNjb3VudCIsInJlYWQ6YXBwcyIsInJlYWQ6Y29ubmVjdGlvbnMiLCJyZWFkOmN1cnJlbnRfdXNlciIsInJlYWQ6aW1wb3J0cyIsInJlYWQ6bGlzdGVkX2FwcHMiLCJyZWFkOm1hcHMiLCJyZWFkOnRpbGVzZXRzIiwicmVhZDp0b2tlbnMiLCJ1cGRhdGU6Y3VycmVudF91c2VyIiwid3JpdGU6YXBwcyIsIndyaXRlOmNvbm5lY3Rpb25zIiwid3JpdGU6aW1wb3J0cyIsIndyaXRlOm1hcHMiLCJ3cml0ZTp0b2tlbnMiXX0.bzeII1lbmjGsWmqN-gmU5dYB3oYhNGm0NeCP_tzWHISgSUXm3_THWn3q-Jbowp893oQyA8EE3RUUbGCi-RUWhoc6RpKeW3jZS4BooeiOu6pO8H5qA7uoy8O9WSxZy7564PJ1YAUEttBEDLzEDKLmo2QSWWXczekFzoSzdqzILgJkqqi2AK-dSXO2eXAfmLW_evffUKWaSjpsIh4RjI3KczIqWucSByXRdQ92iIGtOGbrIoevPWtZljR-4tzMSR6i9a5hah1GExhDAlgN-CC1pW_V1o8O_M7FM--1-5x9UGbChoO0F1Z89mAnAs_kY-lieL09h5n0ROMouFuFmFZfcg',array_construct(" + quotedAndCommaSeparated + "))";
    
    var result = snowflake.execute (
        {sqlText: sqlCommand}
    )
    
    result.next();
    var rows_array = result.getColumnValue(1);
     
    // Insert the rows
    rows_array.forEach((row, idx) => {
        // Get the API results
        var geom = row.geom;
        var search_id = row.search;
        
        // Get the original data and insert the new geocoded row
        sqlCommand = "INSERT INTO " + OUTPUT_TABLE + " SELECT *, TO_GEOGRAPHY('" + JSON.stringify(geom) + "') FROM (" + INPUT + ") WHERE " + GEOCODE_COLUMN + " = '" + search_id +"'";
        snowflake.execute (
            {sqlText: sqlCommand}
        )
    });
    
    return 'Succeed'
    $$;


CALL CARTO_BACKEND_DATA_TEAM.AGRACIANO_TEST.GEOCODE_TABLE_PROC(
'CARTO_BACKEND_DATA_TEAM.AGRACIANO_TEST.TEST_GEOCODING', 
'CARTO_BACKEND_DATA_TEAM.AGRACIANO_TEST.TEST_GEOCODING_OUTPUT', 
'address'
);

SELECT * FROM CARTO_BACKEND_DATA_TEAM.AGRACIANO_TEST.TEST_GEOCODING_OUTPUT