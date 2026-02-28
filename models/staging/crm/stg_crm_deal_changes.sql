{{
    config(
        event_time='changed_at'
    )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('crm', 'deal_changes') }}

),


/*
    Formatted
*/

formatted AS (

    SELECT
        -- SK
        {{ dbt_utils.generate_surrogate_key(['deal_id', 'change_time']) }} AS _surrogate_key,

        -- FK
        CAST(deal_id AS VARCHAR) AS deal_id,

        -- Details
        CAST(changed_field_key AS  VARCHAR) AS changed_field_key,
        CAST(new_value AS  VARCHAR) AS new_value,
            
        -- Metadata
        CAST(change_time AS  TIMESTAMP) AS changed_at

    FROM source_data

)

SELECT * FROM formatted
