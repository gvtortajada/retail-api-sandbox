import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe import convert
from apache_beam.pvalue import AsSingleton

from transformers import CategoriesFn, MapToProduct, MergeProducts, SplitByProductType
from utils import retail_schema
from google.cloud import bigquery


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--products',
        dest='products',
        default='./inputs/products_2.csv',
        help='products input file to process.')
    parser.add_argument(
        '--categories',
        dest='categories',
        default='./inputs/categories.csv',
        help='categories input file to process.')
    parser.add_argument(
        '--bq_table',
        dest='table_spec',
        default="retail-api-337223:retail_api",
        help='BQ table to store the catalog data')
    parser.add_argument(
        '--temp_gcs_bucket',
        dest='temp_gcs_bucket',
        default="gs://test-12345678910112345",
        help='GCS bucket to store BQ load job data')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    primary_products_table_spec = known_args.table_spec + '.primary_products'
    variant_products_table_spec = known_args.table_spec + '.variant_products'
    bq_client = bigquery.Client()
    primary_table_exists = True
    try:
        table_id = primary_products_table_spec.replace(':','.')
        bq_client.get_table(table_id)
    except Exception as e:
        primary_table_exists = False

    variants_table_exists = True
    try:
        table_id = variant_products_table_spec.replace(':','.')
        bq_client.get_table(table_id)
    except Exception:
        variants_table_exists = False


    with beam.Pipeline(options=pipeline_options) as p:

        # Create categories pCollection
        category_df = p | 'Read categories CSV' >> read_csv(
            na_filter=False, path=known_args.categories)
        category_df = (
            convert.to_pcollection(category_df)
            | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))
            | 'Combine to a singleton PCollection' >> beam.CombineGlobally(CategoriesFn())
        )

        # Create Primary and Variant product pCollections
        product_df = p | 'Read products CSV' >> read_csv(
            na_filter=False, path=known_args.products)
        slitted_product_by_type = (
            # Convert the Beam DataFrame to a PCollection.
            convert.to_pcollection(product_df)
            | 'To dict' >> beam.Map(lambda x: dict(x._asdict()))
            | 'Map to key pair' >> beam.Map(lambda x: (x['code'],x))
            | 'Group by code' >> beam.GroupByKey()
            | 'Map to product' >> beam.ParDo(MapToProduct(), AsSingleton(category_df))
            | 'Flatten products' >> beam.FlatMap(lambda x: x[1])
            | 'Split product by type' >> beam.ParDo(SplitByProductType())
                .with_outputs(SplitByProductType.OUTPUT_TAG_VARIANT, main='primary')
        )


        primary_products, _ = slitted_product_by_type
        variants = slitted_product_by_type[SplitByProductType.OUTPUT_TAG_VARIANT]

        current_primary_products = (
            p | 'Create empty primary products when first import' >> beam.Create([])
        )
        if primary_table_exists:
            current_primary_products = (
                p | 'Read primary products from BQ' >> beam.io.ReadFromBigQuery(
                        table=primary_products_table_spec, gcs_location=known_args.temp_gcs_bucket)
                | 'Map BQ primary products to key pair' >> beam.Map(lambda x: (int(x['id']),x))
            )

        current_variants = (
            p | 'Create empty variants when first import' >> beam.Create([])
        )
        if variants_table_exists:
            current_variants = (
                p | 'Read variant product sfrom BQ' >> beam.io.ReadFromBigQuery(
                        table=variant_products_table_spec, gcs_location=known_args.temp_gcs_bucket)
                | 'Map BQ variant products to key pair' >> beam.Map(lambda x: (int(x['id']),x))
            )

        merged_primary_products = (
            ({'current': current_primary_products, 'new': primary_products})
            | 'Merge primary products' >> beam.CoGroupByKey()
            | 'Reduce primary products' >> beam.ParDo(MergeProducts())
        )

        merged_variants = (
            ({'current': current_variants, 'new': variants})
            | 'Merge variant products' >> beam.CoGroupByKey()
            | 'Reduce variant products' >> beam.ParDo(MergeProducts())
        )

        # Write products to BigQuery
        (write_to_BigQuery(merged_primary_products, primary_products_table_spec,
            known_args.temp_gcs_bucket, 'Write to BQ - primary products'))

        write_to_BigQuery(merged_variants, variant_products_table_spec,
            known_args.temp_gcs_bucket, 'Write to BQ - variant products')


def write_to_BigQuery(collection, table_spec, gcs_bucket, transformation_name):
    
        collection | transformation_name >> beam.io.WriteToBigQuery(
                table_spec,
                schema=retail_schema(),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=gcs_bucket)
    


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
