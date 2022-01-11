import json
from typing import List
import configparser
from retail_product import Attribute, AttributeValue, Audience, FulfillmentInfo, Image, PriceInfo, Product, Rating, Ttl

config = configparser.ConfigParser()
config.read('config.INI')
tags = config.get('OPTIONS', 'tags')
if tags:
    tags = tags.split(',')
attributes_key = config.get('OPTIONS', 'attributes_key')
if attributes_key:
    attributes_key = attributes_key.split(',')
attributes_indexable = config.get('OPTIONS', 'attributes_indexable')
if attributes_indexable:
    attributes_indexable = attributes_indexable.split(',')
attributes_searchable = config.get('OPTIONS', 'attributes_searchable')
if attributes_searchable:
    attributes_searchable = attributes_searchable.split(',')
materials = config.get('OPTIONS', 'materials')
if materials:
    materials = materials.split(',')

def fromCSV(line:dict, categories_ref:dict) -> Product:
    product = Product()    
    # product.name = line['summary'] Optional it contains the resource path better to leave it empty
    product.id = line['productCode']
    product.type = 'PRIMARY'
    product.primaryProductId = line['productCode']
    # product.collectionMemberIds = []    
    # product.gtin = line['']
    product.categories = build_categories(line, categories_ref)
    product.title = line['summary']
    # product.brands = line['brand_Id']
    product.description = line['description']
    # product.languageCode = line['']
    product.attributes = build_attributes(line)
    product.tags = build_tags(line)
    product.priceInfo = build_price_info(line)
    # product.rating = build_rating(line)
    # product.expireTime = line['']
    # product.ttl = build_ttl(line)
    # product.availableTime = build_available_time(line)
    product.availability = build_availability(line)
    product.availableQuantity = line['inventory']
    product.fulfillmentInfo = build_fulfillment_info(line)
    product.uri = line['product_global_url']
    product.images = [Image(line['main_sq_gy_800X800'], 800, 800)]
    product.audience = build_audience(line)
    product.sizes = build_sizes(line)
    product.materials = build_materials(line)
    # product.patterns = line['']
    # product.conditions = line['']
    # product.retrievableFields = line['']
    # product.publishTime = line['date_published']
    # product.promotions = line[''] 
    return json.loads(json.dumps(product, default=lambda o: o.__dict__))      

def build_attributes(line: dict) -> List[Attribute]:
    attributes = []
    for key in attributes_key:
        if not line[key] =='':
            value = AttributeValue(text = [line[key]], 
                numbers = [],
                searchable = key in attributes_searchable, 
                indexable = key in attributes_indexable)
            attributes.append(Attribute(key, value))
    return attributes

def build_tags(line: dict) -> List[str]:
    return list(filter(None, [line[tag] for tag in tags]))

def build_price_info(line: dict) -> PriceInfo:
    return PriceInfo('USD', line['salePrice'], line['listPrice'], None, None, None)


def build_rating(line: dict) -> Rating:
    return None


def build_ttl(line: dict) -> Ttl:
    return None


def build_fulfillment_info(line: dict) -> List[FulfillmentInfo]:
    return []


def build_audience(line: dict) -> Audience:
    return None

    
def build_sizes(line: dict) -> List[str]:
    return [size.split(':')[1] for size in line['sizes'].split(',')]


def build_materials(line: dict) -> List[str]:
    return list(filter(None, [line[material] for material in materials]))


def build_category_path(category_id:str, accumulator: List[str], categories_ref: dict):
    category = categories_ref[category_id]
    if not category['name'] == '':
        accumulator.insert(0,category['name'])
    if not category['super_category'] == '1':
        build_category_path(category['super_category'], accumulator, categories_ref)


def build_categories(line: dict, categories_ref: dict) -> List[str]:
    if line['category'] == '':
        line['category'] = line['categoryGallery']

    category_path_list = []
    for category in line['category'].split(','):
        accumulator = []
        build_category_path(category, accumulator, categories_ref)
        accumulator = ' > '.join(accumulator)
        category_path_list.append(accumulator)

    return category_path_list

def build_availability(line: dict) -> str:
    return 'IN_STOCK' if line['inventory'] > 0 else 'OUT_OF_STOCK'

def build_available_time(line: dict) -> str:
    return line['date_published']

def retail_schema():
    return {
    'fields':[
  {
    "name": "name",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "type",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "primaryProductId",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "collectionMemberIds",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "gtin",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "categories",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "title",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "brands",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "description",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "languageCode",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "attributes",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
      {
        "name": "key",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "value",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
          {
            "name": "text",
            "type": "STRING",
            "mode": "REPEATED"
          },
          {
            "name": "numbers",
            "type": "FLOAT",
            "mode": "REPEATED"
          },
          {
            "name": "searchable",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
          },
          {
            "name": "indexable",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
          }
        ]
      }
    ]
  },
  {
    "name": "tags",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "priceInfo",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "currencyCode",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "price",
        "type": "FLOAT",
        "mode": "NULLABLE"
      },
      {
        "name": "originalPrice",
        "type": "FLOAT",
        "mode": "NULLABLE"
      },
      {
        "name": "cost",
        "type": "FLOAT",
        "mode": "NULLABLE"
      },
      {
        "name": "priceEffectiveTime",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "priceExpireTime",
        "type": "STRING",
        "mode": "NULLABLE"
      }
    ]
  },
  {
    "name": "rating",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "ratingCount",
        "type": "INTEGER",
        "mode": "NULLABLE"
      },
      {
        "name": "averageRating",
        "type": "FLOAT",
        "mode": "NULLABLE"
      },
      {
        "name": "ratingHistogram",
        "type": "INTEGER",
        "mode": "REPEATED"
      }
    ]
  },
  {
    "name": "expireTime",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "ttl",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "seconds",
        "type": "INTEGER",
        "mode": "NULLABLE"
      },
      {
        "name": "nanos",
        "type": "INTEGER",
        "mode": "NULLABLE"
      }
    ]
  },
  {
    "name": "availableTime",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "availability",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "availableQuantity",
    "type": "INTEGER",
    "mode": "NULLABLE"
  },
  {
    "name": "fulfillmentInfo",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
      {
        "name": "type",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "placeIds",
        "type": "STRING",
        "mode": "REPEATED"
      }
    ]
  },
  {
    "name": "uri",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "images",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
      {
        "name": "uri",
        "type": "STRING",
        "mode": "REQUIRED"
      },
      {
        "name": "height",
        "type": "INTEGER",
        "mode": "NULLABLE"
      },
      {
        "name": "width",
        "type": "INTEGER",
        "mode": "NULLABLE"
      }
    ]
  },
  {
    "name": "audience",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "genders",
        "type": "STRING",
        "mode": "REPEATED"
      },
      {
        "name": "ageGroups",
        "type": "STRING",
        "mode": "REPEATED"
      }
    ]
  },
  {
    "name": "colorInfo",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "colorFamilies",
        "type": "STRING",
        "mode": "REPEATED"
      },
      {
        "name": "colors",
        "type": "STRING",
        "mode": "REPEATED"
      }
    ]
  },
  {
    "name": "sizes",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "materials",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "patterns",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "conditions",
    "type": "STRING",
    "mode": "REPEATED"
  },
  {
    "name": "retrievableFields",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "publishTime",
    "type": "STRING",
    "mode": "NULLABLE"
  },
  {
    "name": "promotions",
    "type": "RECORD",
    "mode": "REPEATED",
    "fields": [
      {
        "name": "promotionId",
        "type": "STRING",
        "mode": "NULLABLE"
      }
    ]
  }
]
}