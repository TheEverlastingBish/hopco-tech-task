{{ 
    config(
        tag='raw'
    )
}}


WITH final AS (
    SELECT
        id,
        polymorphic_type,
        created_time,
        modified_time,
        content_slug
    FROM
        {{ source('hopco_ops', 'base_app_result') }}
)

SELECT
    * 
FROM
    final
