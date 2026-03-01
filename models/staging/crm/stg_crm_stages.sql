/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('crm', 'stages') }}

),


/*
    Formatted
*/


formatted AS (

    SELECT
        -- PK
        CAST(stage_id AS INT) AS stage_id,

        -- Details
        CAST(stage_name AS  VARCHAR) AS stage_name

    FROM source_data

)

SELECT * FROM formatted
