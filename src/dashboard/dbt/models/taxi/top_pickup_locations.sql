{{ config(materialized='external', location="s3://{{ env_var('S3_BUCKET') }}/dashboard/my_view.parquet") }}

SELECT 
    location_id,
    counts
FROM
    {{ ref('trips_by_pickup_location') }}
ORDER BY 2 DESC
LIMIT 200