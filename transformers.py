import apache_beam as beam
from utils import fromCSV


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
        yield fromCSV(element, categories)

class MergeProducts(beam.DoFn):
    def process(self, element):
        if not element[1]['new']:
            current = element[1]['current'][0]
            current['availability'] = 'OUT_OF_STOCK'
            current['availableQuantity'] = 0
            yield current
        else:
            yield element[1]['new'][0]
