import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe import convert
from apache_beam.pvalue import AsSingleton

from transformers import CategoriesFn, MapToProduct
from utils import retail_schema


def run(argv=None, save_main_session=True):
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
        '--bq_table',
        dest='table_spec',
        default="",
        help='BQ table to store the catalog data')
    parser.add_argument(
        '--temp_gcs_bucket',
        dest='temp_gcs_bucket',
        default="gs://...",
        help='GCS bucket to store BQ load job data')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session
        
    with beam.Pipeline(options=pipeline_options) as p:

        category_df = p | 'Read categories CSV' >> read_csv(
            na_filter=False, path=known_args.categories)
        category_df = (
            # Convert the Beam DataFrame to a PCollection.
            convert.to_pcollection(category_df)
            | 'To dictionaries' >> beam.Map(lambda x: dict(x._asdict()))
            | 'Combine to a singleton PCollection' >> beam.CombineGlobally(CategoriesFn())
        )

        product_df = p | 'Read products CSV' >> read_csv(
            na_filter=False, path=known_args.products)
        (
            # Convert the Beam DataFrame to a PCollection.
            convert.to_pcollection(product_df)
            | 'To dict' >> beam.Map(lambda x: dict(x._asdict()))
            | 'Map to product' >> beam.ParDo(MapToProduct(), AsSingleton(category_df))
            | 'Write to BQ' >> beam.io.WriteToBigQuery(
                    known_args.table_spec,
                    schema=retail_schema(),
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=known_args.temp_gcs_bucket)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
