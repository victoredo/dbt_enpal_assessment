{{
    config(
        event_time='modified_at'
    )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('crm', 'users') }}

),


/*
    Formatted
*/

formatted AS (

    SELECT
        -- PK
        CAST(id AS VARCHAR) AS user_id,

        -- Details
        {{ sha256("email") }} AS user_email,
        CAST(name AS  VARCHAR) AS user_name,
            
        -- Metadata
        CAST(modified AS  TIMESTAMP) AS modified_at

    FROM source_data

)

SELECT * FROM formatted
