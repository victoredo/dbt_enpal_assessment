/*
    Tables
*/

WITH
all_fields AS (

    SELECT * FROM {{ ref('stg_crm_fields') }}
),

/*
    Transformations
*/

scalar_fields AS (

    -- Fields with no enum options — pass through with null option columns
    SELECT
        field_id,
        field_key,
        field_name,
        null::VARCHAR  AS option_id,
        null::VARCHAR  AS option_label,
        null::INT   AS option_order
    FROM all_fields
    WHERE field_value_options IS NULL

),

enum_fields as (

    select
        all_fields.field_id,
        all_fields.field_key,
        all_fields.field_name,

        -- Extract scalar values from each JSON object in the array
        (option_element ->> 'id')::VARCHAR             AS option_id,
        (option_element ->> 'label')::VARCHAR          AS option_label,

        -- WITH ORDINALITY gives 1-based index — subtract 1 for 0-based order
        (option_index - 1)::INT                     AS option_order

    FROM all_fields,

        -- json_array_elements expands the JSON array into one row per element
        -- WITH ORDINALITY preserves original array position for display ordering
        json_array_elements(
            field_value_options::JSON
        ) WITH ordinality AS t(option_element, option_index)

    WHERE field_value_options IS NOT NULL

),

unioned AS (

    SELECT * FROM scalar_fields
    UNION ALL
    SELECT * FROM enum_fields

),

/*
    Formatted
*/

formatted AS (

    SELECT 
        -- SK
        {{ dbt_utils.generate_surrogate_key(['field_id', 'option_id']) }} AS _surrogate_key,

        -- FK
        field_id,
        option_id,

        -- Details
        field_key,
        field_name,
        option_label,

        -- Metadata
        option_order

    FROM unioned
)

SELECT * FROM formatted
