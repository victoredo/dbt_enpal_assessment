/*
  int_deal_stage_transitions
  ─────────────────────────────────────────────────────────────────────────────
  Materialization: view
  Incremental: NOT applied at this layer — intermediate models are views that
  sit on top of staging. Incremental logic is applied at the fact layer
  (fct_deal_changes) which this model feeds into. Pushing incremental to the
  fact layer keeps intermediate models simple and testable.
*/

/*
    Tables
*/
WITH deal_changes AS (

    SELECT * FROM {{ ref('stg_crm_deal_changes') }}

),

stages AS (

    SELECT * FROM {{ ref('stg_crm_stages') }}

),

/*
    Transformations
*/

-- Isolate only stage_id change events — these are the funnel transition rows
stage_changes AS (

    SELECT
        deal_id,
        new_value::INT                              AS stage_id,
        changed_at                                  AS entered_stage_at

    FROM deal_changes
    WHERE changed_field_key = 'stage_id'

),

-- Enrich with stage name so downstream models never need to re-join stages
enriched AS (

    SELECT
        stage_changes.deal_id,
        stage_changes.stage_id,
        stages.stage_name,
        stage_changes.entered_stage_at,

        -- Pre-truncate month here — used directly in mart aggregations
        date_trunc('month', stage_changes.entered_stage_at)::date  AS entered_stage_month

    FROM stage_changes
    LEFT JOIN stages
        ON stage_changes.stage_id = stages.stage_id

),

/*
    Formatted
*/

formatted AS (

    SELECT 
        --SK
        {{ dbt_utils.generate_surrogate_key(['deal_id', 'stage_id','entered_stage_at']) }} AS _surrogate_key,
        -- FK
        deal_id,
        stage_id,

        -- Details
        stage_name,
    
        -- Metadata
        entered_stage_month,
        entered_stage_at

    FROM enriched
)

SELECT * FROM formatted
