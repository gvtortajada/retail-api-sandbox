import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe import convert
from apache_beam.pvalue import AsSingleton
from google.cloud import bigquery
from retailapiloader.bq_schema import retail_schema
from retailapiloader.transformers import CategoriesFn, MapToProduct, MergeProducts
from retailapiloader.utils import Utils


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--products',
        dest='products',
        default='./inputs/products.csv',
        help='products input file to process.')
    parser.add_argument(
        '--categories',
        dest='categories',
        default='./inputs/categories.csv',
        help='categories input file to process.')
    parser.add_argument(
        '--bq_dataset',
        dest='bq_dataset',
        default="retail-api-337223:retail_api",
        help='BQ dataset to store the catalog data')
    parser.add_argument(
        '--temp_gcs_bucket',
        dest='temp_gcs_bucket',
        default="gs://temp-import-retail-api",
        help='GCS bucket to store BQ load job data')
    parser.add_argument(
        '--config_file_gsutil_uri',
        dest='config_file_gsutil_uri',
        default="gs://retail-api-row-catalog-data/config.INI",
        help='gsutil uri of the config file')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    utils = Utils(pipeline_options, known_args)
    
    products_table_spec = known_args.bq_dataset + '.catalog'
    logging.info('Using BigQuery table_spec: '+products_table_spec)

    bq_client = bigquery.Client()
    table_exists = True
    try:
        table_id = products_table_spec.replace(':','.')
        bq_client.get_table(table_id)
    except Exception as e:
        table_exists = False

    with beam.Pipeline(options=pipeline_options) as p:

        categories = p | 'Read categories CSV' >> read_csv(
            na_filter=False, path=known_args.categories)
        categories = (
            # Convert the Beam DataFrame to a PCollection.
            convert.to_pcollection(categories)
            | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))
            | 'Combine to a singleton PCollection' >> beam.CombineGlobally(CategoriesFn())
        )

        previous_products = (
            p | 'Create empty products when first import' >> beam.Create([])
        )
        if table_exists:
            previous_products = (
                p | 'Read previous products from BQ' >> beam.io.ReadFromBigQuery(
                        table=products_table_spec, gcs_location=known_args.temp_gcs_bucket)
                | 'Map previous products to key pair' >> beam.Map(lambda x: (int(x['id']),x))
            )

        current_products = p | 'Read products CSV' >> read_csv(
            na_filter=False, path=known_args.products)
        current_products = (
            # Convert the Beam DataFrame to a PCollection.
            convert.to_pcollection(current_products)
            | 'To dict' >> beam.Map(lambda x: dict(x._asdict()))
            | 'Map to product' >> beam.ParDo(MapToProduct(utils), AsSingleton(categories))
            | 'Map current products to key pair' >> beam.Map(lambda x: (int(x['id']),x))
        )

        merged_products = (
            ({'current': previous_products, 'new': current_products})
            | 'Merge primary products' >> beam.CoGroupByKey()
            | 'Reduce primary products' >> beam.ParDo(MergeProducts())
        )

        # Write products to BigQuery
        (write_to_BigQuery(merged_products, products_table_spec,
            known_args.temp_gcs_bucket, 'Write to BQ - products'))

        p.run().wait_until_finish()


def write_to_BigQuery(collection, table_spec, gcs_bucket, transformation_name):

        collection | transformation_name >> beam.io.WriteToBigQuery(
                table_spec,
                schema=retail_schema(),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=gcs_bucket)