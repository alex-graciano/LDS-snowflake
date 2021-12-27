CREATE OR REPLACE api integration api_integration_lds
    api_provider = aws_api_gateway
    api_aws_role_arn = 'arn:aws:iam::*******:role/lds-snowflake'
    enabled = true
    api_allowed_prefixes = ('https://******.execute-api.eu-west-3.amazonaws.com/dev/geocode-table');
    
    
DESCRIBE integration api_integration_lds;


CREATE OR REPLACE EXTERNAL FUNCTION geocode_table_test
(addresses_array VARIANT)
RETURNS VARIANT
api_integration = api_integration_lds
AS 'https://*****.execute-api.eu-west-3.amazonaws.com/dev/geocode-table'


select geocode_table_test(array_construct('Cruz, Granada', 'Padre Poveda, Jaen'))