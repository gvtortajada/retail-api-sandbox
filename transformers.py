import apache_beam as beam
from apache_beam import pvalue
from utils import variants_and_primary_from_CSV


class CategoriesFn(beam.CombineFn):
    def create_accumulator(self):
        return {}

    def add_input(self, accumulator, input):
        if str(input['code']) not in accumulator:
            accumulator[str(input['code'])] = input
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = {}
        for accum in accumulators:
            merged.update(accum)
        return merged

    def extract_output(self, accumulator):
        return accumulator


class MapToProduct(beam.DoFn):
    def process(self, element, categories):
        yield variants_and_primary_from_CSV(element, categories)


class SplitByProductType(beam.DoFn):
    OUTPUT_TAG_VARIANT = 'OUTPUT_TAG_VARIANT'
    def process(self, element):
        output = (element['id'], element)
        if element['type'] == 'VARIANT':
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_VARIANT, output)
        else:
            yield output


class MergeProducts(beam.DoFn):
    def process(self, element):
        if not element[1]['new']:
            current = element[1]['current'][0]
            current['availability'] = 'OUT_OF_STOCK'
            current['availableQuantity'] = 0
            yield current
        else:
            yield element[1]['new'][0]

