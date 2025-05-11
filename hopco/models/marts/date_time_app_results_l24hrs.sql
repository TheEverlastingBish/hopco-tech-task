{{
    config(
        tags=['app', 'patient', 'result'],
        materialized='table'
    )
}}


SELECT
    br.* EXCLUDE(id),
    dtar.result_ptr_id,
    TO_TIMESTAMP(
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f") }}'
    ) AS etl_run_started_ts,
    HASH(concat(*)) AS hash_id  -- PK
FROM
    {{ ref('int__app_results_history') }} AS br
INNER JOIN
    {{ ref('stg__date_time_app_results') }} AS dtar
    ON br.id = dtar.result_ptr_id
WHERE
    modified_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY modified_time DESC
    ) = 1
ORDER BY
    modified_time DESC
