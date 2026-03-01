
/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('crm', 'activity_types') }}

),


/*
    Formatted
*/

formatted AS (

    SELECT
        -- PK
        CAST(id AS VARCHAR) AS activity_type_id,

        -- Details
        CAST(type AS VARCHAR) AS activity_type_key,
        CAST(name AS VARCHAR) AS activity_type_name,
        CASE 
            WHEN lower(active) = 'yes' 
                THEN true ELSE false
        END as is_active

    FROM source_data

)

SELECT * FROM formatted
