{{
    config(
        tags=['app', 'patient', 'result'],
    )
}}


WITH final AS (
    SELECT
        hash_id,
        id AS result_ptr_id,
        polymorphic_type,
        created_time,
        modified_time,
        content_slug,
        integer_value,
    FROM
        {{ ref('int__app_results_history') }}
    WHERE
        modified_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        AND polymorphic_type = 'IntegerAppResult'
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY modified_time DESC
        ) = 1
    ORDER BY
        modified_time DESC
)

SELECT
    hash_id,
    result_ptr_id,
    polymorphic_type,
    created_time,
    modified_time,
    content_slug,
    integer_value,
FROM 
    final
