{{
    config(
        tags=['app', 'patient', 'result'],
        cluster_by=['POLYMORPHIC_TYPE']
    )
}}


WITH final AS (
    SELECT * 
    FROM
        {{ ref('int__app_results_history') }}
    QUALIFY 
        ROW_NUMBER() OVER (
            PARTITION BY record_id
            ORDER BY modified_time DESC
        ) = 1
)

SELECT * 
FROM
    final
