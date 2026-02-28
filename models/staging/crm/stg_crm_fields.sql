/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('crm', 'fields') }}

),


/*
    Formatted
*/


formatted AS (

    SELECT
       CAST(id AS VARCHAR) AS field_id,
       CAST(field_key AS  VARCHAR) AS field_key,
       CAST(name AS  VARCHAR) AS field_name,
        

        -- Cast to text first, nullify empty strings, then clean double-escaped
        -- CSV quotes (""key"") to produce valid JSON for downstream casting
       CAST( replace(
            nullif(field_value_options::VARCHAR, ''),
            '""', '"'
        )   AS JSON)      AS field_value_options_raw

    from source_data

)

SELECT * FROM formatted