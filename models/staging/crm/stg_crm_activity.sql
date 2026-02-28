{{
    config(
        event_time='due_at'
    )
}}

/*
    Tables
*/

WITH source_data AS (

    SELECT * FROM {{ source('crm', 'activity') }}

),


/*
    Formatted
*/

formatted AS (

    SELECT
        -- SK
        {{ dbt_utils.generate_surrogate_key(['activity_id', 'deal_id']) }} AS _surrogate_key,

        -- FK
        CAST(activity_id AS VARCHAR) AS activity_id,
        CAST(deal_id AS VARCHAR) AS deal_id,

        -- Details
        CAST(assigned_to_user AS VARCHAR) AS user_id,
        CAST(type AS VARCHAR) AS activity_type_key,
        CAST(done AS BOOLEAN) AS is_done,
            
        -- Metadata
        CAST(due_to AS  TIMESTAMP) AS due_at

    FROM source_data

)

SELECT * FROM formatted
