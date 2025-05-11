{{
    config(
        tags=['app', 'patient', 'result'],
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        unique_key='record_id',
        on_schema_change="append_new_columns",
        incremental_predicates=[
            "DBT_INTERNAL_DEST.modified_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())"
        ],
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
