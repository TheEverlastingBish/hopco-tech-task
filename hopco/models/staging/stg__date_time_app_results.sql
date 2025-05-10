{{ 
    config(
        tag='raw'
    )
}}


WITH final AS (
    SELECT
        result_ptr_id,
        "VALUE" AS datetime_value
    FROM
        {{ source('hopco_ops', 'date_time_app_result') }}
)

SELECT
    * 
FROM
    final
