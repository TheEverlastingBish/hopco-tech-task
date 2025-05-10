{{
    config(
        tags=['app', 'patient', 'result'],
        materialized='incremental',
        cluster_by=['POLYMORPHIC_TYPE']
    )
}}


SELECT *
FROM
    {{ ref('int__app_results_history') }}
WHERE
    modified_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY
            id,
            polymorphic_type
        ORDER BY
            modified_time DESC
    ) = 1
ORDER BY
    modified_time DESC
