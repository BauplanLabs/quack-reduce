SELECT 
    pickup_location_id AS location_id, 
    COUNT(*) AS counts 
FROM 
    read_parquet(['s3://{{ env_var('S3_BUCKET_NAME') }}/dataset/taxi_2019_04.parquet']) 
GROUP BY 1