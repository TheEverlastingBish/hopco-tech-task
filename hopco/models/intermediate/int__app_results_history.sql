{{
    config(
        tags=['app', 'patient', 'result'],
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        unique_key='table_pk',
        on_schema_change="append_new_columns",
        incremental_predicates=[
                "DBT_INTERNAL_DEST.modified_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())"
        ],
        cluster_by=['POLYMORPHIC_TYPE']
    )
}}


WITH final AS (
    SELECT
        br.* EXCLUDE(id),
        br.id AS result_ptr_id,
        dtar.datetime_value,
        iar.integer_value,
        rar.range_start,
        rar.range_end,

        TO_TIMESTAMP(
            '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}'
        ) AS etl_run_started_ts,

        {{ dbt_utils.generate_surrogate_key([
            'br.id', 'br.polymorphic_type'
        ]) }} AS record_id,

        {{ dbt_utils.generate_surrogate_key([
            'br.id', 'br.polymorphic_type', 'br.modified_time'
        ]) }} AS table_pk
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
)

SELECT *
FROM
    final
