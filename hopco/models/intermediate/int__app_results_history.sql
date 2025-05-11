{{
    config(
        tags=['app', 'patient', 'result'],
        unique_key='hash_id',
        materialized='incremental',
        on_schema_change='append_new_columns',
        cluster_by=['POLYMORPHIC_TYPE']
    )
}}


SELECT
    br.*,
    datetime_value,
    integer_value,
    range_start,
    range_end,

    TO_TIMESTAMP(
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}'
    ) AS etl_run_started_ts,

    {{ dbt_utils.generate_surrogate_key([
        'br.id', 'br.polymorphic_type', 'br.content_slug', 'br.modified_time', 
        'datetime_value', 'integer_value', 'range_start', 'range_end'
    ]) }} AS hash_id  -- PK

FROM
    {{ ref('stg__base_app_results') }} AS br
LEFT JOIN
    {{ ref('stg__date_time_app_results') }} AS dtar
    ON br.id = dtar.result_ptr_id
LEFT JOIN
    {{ ref('stg__integer_app_results') }} AS iar
    ON br.id = iar.result_ptr_id
LEFT JOIN
    {{ ref('stg__range_app_results') }} AS  rar
    ON br.id = rar.result_ptr_id
