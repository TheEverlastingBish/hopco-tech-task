-- Stash


MERGE INTO hopco.app_result AS br
USING app_result AS src
	ON br.id = src.id
	AND br.polymorphic_type = src.polymorphic_type
	AND src.modified_time >= CURRENT_TIMESTAMP() - interval '24 hours'
WHEN NOT MATCHED BY TARGET THEN
INSERT VALUES
	(
		id,
		polymorphic_type,
		created_time,
		modified_time,
		content_slug,
	)
WHEN MATCHED
	AND src.modified_time > br.modified_time
UPDATE
	SET
		br.modified_time = src.modified_time,
		br.content_slug = src.content_slug,
WHEN NOT MATCHED BY SOURCE
THEN DELETE;


TRUNCATE TABLE hopco.date_time_app_result;

INSERT INTO hopco.date_time_app_result
SELECT
    result_ptr_id,
    "value"::datetime
FROM date_time_app_result AS src
INNER JOIN app_result AS ar
    ON ar.id = src.id
    AND ar.polymorphic_type = src.polymorphic_type
    AND ar.modified_time >= CURRENT_TIMESTAMP() - interval '24 hours'


SELECT
    table_schema,
    table_name,
    column_name,
    ordinal_position AS column_ordinal,
    data_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'BASE_APP_RESULT'
ORDER BY ORDINAL_POSITION ASC
LIMIT 20;




SELECT
    br.*,
    datetime_value,
    integer_value,
    range_start,
    range_end
FROM 
    HOPCO.PUBLIC_STAGING.STG__BASE_APP_RESULTS AS br
LEFT JOIN
    HOPCO.PUBLIC_STAGING.STG__DATE_TIME_APP_RESULTS AS dtar
    ON br.id = dtar.result_ptr_id
LEFT JOIN
    HOPCO.PUBLIC_STAGING.STG__INTEGER_APP_RESULTS AS iar
    ON br.id = iar.result_ptr_id
LEFT JOIN
    HOPCO.PUBLIC_STAGING.STG__RANGE_APP_RESULTS AS  rar
    ON br.id = rar.result_ptr_id
WHERE
    br.modified_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
ORDER BY
    modified_time DESC
;


SELECT
    dtar.result_ptr_id,
    dtar.value AS datetime_value,
    br.polymorphic_type,
    br.created_time,
    br.modified_time,
    br.content_slug,
    TIMESTAMP(
        '{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f UTC") }}'
    ) AS etl_run_started_ts,
    HASH(concat(*)) AS hash_id  -- PK
FROM 
    HOPCO.PUBLIC_STAGING.STG__BASE_APP_RESULTS AS br
LEFT JOIN
    HOPCO.PUBLIC_STAGING.STG__DATE_TIME_APP_RESULTS AS dtar
    ON br.id = dtar.result_ptr_id
    AND br.polymorphic_type = 'DateTimeAppResult'
;


WITH base AS (
    SELECT
        br.*,
        dtar.value AS datetime_value,
        iar.value AS integer_value,
        rar.value AS range_start,
        rar.value AS range_end
    FROM 
        HOPCO.PUBLIC_STAGING.STG__BASE_APP_RESULTS AS br
    LEFT JOIN
        HOPCO.PUBLIC_STAGING.STG__DATE_TIME_APP_RESULTS AS dtar
        ON br.id = dtar.result_ptr_id
    LEFT JOIN
        HOPCO.PUBLIC_STAGING.STG__INTEGER_APP_RESULTS AS iar
        ON br.id = iar.result_ptr_id
    LEFT JOIN
        HOPCO.PUBLIC_STAGING.STG__RANGE_APP_RESULTS AS  rar
        ON br.id = rar.result_ptr_id
)

SELECT
FROM base
PIVOT(
    MAX() FOR quarter IN (ANY ORDER BY quarter))
  ORDER BY empid;

