{{
    config(
        tags=['app', 'patient', 'result'],
        materialized='incremental',
        cluster_by=['POLYMORPHIC_TYPE']
    )
}}


SELECT
    br.*,
    datetime_value,
    integer_value,
    range_start,
    range_end
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
