# installation
```
git clone https://github.com/gvtortajada/retail-api-sandbox
cd retail-api-sandbox
pip install virtualenv
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
```

# configuration
Configure tags, attributes and materials in config.INI file


# run locally
```
gcloud init
gcloud auth application-default login
python pipeline-files.py \
    --products <path to product csv file> \
    --categories <path to categories csv file> \
    --bq_table <target BQ table spec (project_id:dataset.table)> \
    --temp_gcs_bucket <GCS bucket to store BQ load job data>

```
