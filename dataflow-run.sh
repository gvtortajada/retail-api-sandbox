python main.py \
    --runner=DataflowRunner \
    --setup_file ./setup.py \
    --products gs://retail-api-row-catalog-data/products.csv \
    --categories gs://retail-api-row-catalog-data/categories.csv \
    --bq_dataset retail-api-337223:retail_api \
    --temp_gcs_bucket gs://temp-import-retail-api \
    --project retail-api-337223 \
    --region northamerica-northeast1 \
    --temp_location gs://temp-import-retail-api/tmp \
    --config_file_gsutil_uri gs://retail-api-row-catalog-data/config.INI

