{{ 
    config(
        tag='raw'
    )
}}


WITH final AS (
    SELECT
        result_ptr_id,
        "from" AS range_start,
        "to" AS range_end
    FROM
        {{ source('hopco_ops', 'range_app_result') }}
)

SELECT
    * 
FROM
    final
