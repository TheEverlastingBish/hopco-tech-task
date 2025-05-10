{{ 
    config(
        tag='raw'
    )
}}


WITH final AS (
    SELECT
        result_ptr_id,
        "VALUE" AS integer_value
    FROM
        {{ source('hopco_ops', 'integer_app_result') }}
)

SELECT
    * 
FROM
    final
