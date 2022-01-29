from argparse import Namespace
import json
import logging
from typing import List
import configparser
from apache_beam.options.pipeline_options import PipelineOptions
from retailapiloader.retail_product import Attribute, AttributeValue, Audience, FulfillmentInfo, Image, PriceInfo, Product, Rating, Ttl
from google.cloud import storage

class Utils:

    def __init__(self, pipeline_options: PipelineOptions, known_args: Namespace):
        options = pipeline_options.get_all_options()
        if options['runner']:
            client = storage.Client()
            config_uri = known_args.config_file_gsutil_uri  
            with open('config.INI', 'wb') as config_file:
                client.download_blob_to_file(config_uri, config_file)
        config = configparser.ConfigParser()
        config.read('config.INI')
        self.tags = config.get('OPTIONS', 'tags')
        if self.tags:
            self.tags = self.tags.split(',')
        self.attributes_key = config.get('OPTIONS', 'attributes_key')
        if self.attributes_key:
            self.attributes_key = self.attributes_key.split(',')
        self.attributes_indexable = config.get('OPTIONS', 'attributes_indexable')
        if self.attributes_indexable:
            self.attributes_indexable = self.attributes_indexable.split(',')
        self.attributes_searchable = config.get('OPTIONS', 'attributes_searchable')
        if self.attributes_searchable:
            self.attributes_searchable = self.attributes_searchable.split(',')
        self.materials = config.get('OPTIONS', 'materials')
        if self.materials:
            self.materials = self.materials.split(',')

    def fromCSV(self, line:dict, categories_ref:dict) -> Product:
        product = Product()    
        # product.name = line['summary'] Optional it contains the resource path better to leave it empty
        product.id = line['productCode']
        product.type = 'PRIMARY'
        product.primaryProductId = line['productCode']
        # product.collectionMemberIds = []    
        # product.gtin = line['']
        product.categories = self.build_categories(line, categories_ref)
        product.title = line['summary']
        # product.brands = line['brand_Id']
        product.description = line['description']
        # product.languageCode = line['']
        product.attributes = self.build_attributes(line)
        product.tags = self.build_tags(line)
        product.priceInfo = self.build_price_info(line)
        # product.rating = build_rating(line)
        # product.expireTime = line['']
        # product.ttl = build_ttl(line)
        # product.availableTime = build_available_time(line)
        product.availability = self.build_availability(line)
        product.availableQuantity = line['inventory']
        product.fulfillmentInfo = self.build_fulfillment_info(line)
        product.uri = line['product_global_url']
        product.images = [Image(line['main_sq_gy_800X800'], 800, 800)]
        product.audience = self.build_audience(line)
        product.sizes = self.build_sizes(line)
        product.materials = self.build_materials(line)
        # product.patterns = line['']
        # product.conditions = line['']
        # product.retrievableFields = line['']
        # product.publishTime = line['date_published']
        # product.promotions = line[''] 
        return json.loads(json.dumps(product, default=lambda o: o.__dict__))      

    def build_attributes(self, line: dict) -> List[Attribute]:
        attributes = []
        for key in self.attributes_key:
            if not line[key] =='':
                value = AttributeValue(text = [line[key]], 
                    numbers = [],
                    searchable = key in self.attributes_searchable, 
                    indexable = key in self.attributes_indexable)
                attributes.append(Attribute(key, value))
        return attributes

    def build_tags(self, line: dict) -> List[str]:
        return list(filter(None, [line[tag] for tag in self.tags]))

    def build_price_info(self, line: dict) -> PriceInfo:
        return PriceInfo('USD', line['salePrice'], line['listPrice'], None, None, None)


    def build_rating(self, line: dict) -> Rating:
        return None


    def build_ttl(self, line: dict) -> Ttl:
        return None


    def build_fulfillment_info(self, line: dict) -> List[FulfillmentInfo]:
        return []


    def build_audience(lself, ine: dict) -> Audience:
        return None

        
    def build_sizes(self, line: dict) -> List[str]:
        try:
            sizes = line['sizes']
            if not sizes or 'One Size' in sizes:
                return []
            return [size.split(':')[1] for size in sizes.split(',')]
        except:
            print(line['sizes'])


    def build_materials(self, line: dict) -> List[str]:
        return list(filter(None, [line[material] for material in self.materials]))


    def build_category_path(self, category_id:str, accumulator: List[str], categories_ref: dict):
        category = categories_ref[category_id]
        if not category['name'] == '':
            accumulator.insert(0,category['name'])
        if not category['super_category'] == '1':
            self.build_category_path(category['super_category'], accumulator, categories_ref)


    def build_categories(self, line: dict, categories_ref: dict) -> List[str]:
        if line['category'] == '':
            line['category'] = line['categoryGallery']

        category_path_list = []
        for category in line['category'].split(','):
            accumulator = []
            self.build_category_path(category, accumulator, categories_ref)
            accumulator = ' > '.join(accumulator)
            category_path_list.append(accumulator)

        return category_path_list

    def build_availability(self, line: dict) -> str:
        return 'IN_STOCK' if line['inventory'] > 0 else 'OUT_OF_STOCK'

    def build_available_time(self, line: dict) -> str:
        return line['date_published']

