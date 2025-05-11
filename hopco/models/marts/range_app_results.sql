{{
    config(
        tags=['app', 'patient', 'result'],
    )
}}


WITH final AS (
    SELECT
        record_id,
        result_ptr_id,
        polymorphic_type,
        created_time,
        modified_time,
        content_slug,
        range_start,
        range_end,
        etl_run_started_ts,
        table_pk,
    FROM
        {{ ref('int__app_results') }}
    WHERE
        polymorphic_type = 'RangeAppResult'
)

SELECT * 
FROM 
    final
