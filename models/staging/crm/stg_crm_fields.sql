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
        -- PK
        CAST(id AS VARCHAR) AS field_id,

        -- Details
        CAST(field_key AS  VARCHAR) AS field_key,
        CAST(name AS  VARCHAR) AS field_name,
            
        -- Metadata
        CAST(field_value_options AS JSON) AS  field_value_options

    FROM source_data

)

SELECT * FROM formatted
