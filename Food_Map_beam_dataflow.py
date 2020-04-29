import datetime,logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import pandas as pd
#!pip install noms
import noms
client = noms.Client("lMekhvK6sRaykDk7sov61ZEuJK06faPSspjcg8En")

#Want to map all food groups from USDA to relevant food items from the instacart product table
#select product_name, department, d.department_id from instacart_modeled.Products p
#inner join instacart_modeled.Departments d on d.department_id = p.department_id
#where d.department_id not in (1,2,5,11,12,17,18,19)
#and p.product_name not like '%Filters%'
#order by product_name



def veg_check(product, type_v):
    if product is None:
        return False
    if type_v == 'green':
        for veg in green_veggies:
            if veg in product or product in veg:
                return True

    elif type_v == 'orange':
        for veg in orange_veggies:
            if veg in product or product in veg:
                return True

    elif type_v == 'starchy':
        for veg in starchy_veggies:
            if veg in product or product in veg:
                return True
    return False
    
        
# find the USDA food item that best matches the product
class MatchProductFn(beam.DoFn):  
    def process(self,element):
        food_id_map = {8: 47, 43: 47, 93: 47, 112: 19, 128: 19, 26: 54, 31: 41, 64: 41, 77: 41, 90: 42, 94: 54, 98: 3, 115: 53, 48: 46, 57: 20, 121: 20, 130: 45, 18: 14, 68: 19, 59: 49, 69: 49, 81: 13, 95: 30, 99: 2, 2: 26, 21: 26, 36: 38, 53: 27, 71: 27, 84: 25, 86: 37, 91: 27, 108: 27, 120: 27, 1: 52, 13: 52, 14: 0, 67: 52, 96: 52, 4: 19, 9: 7, 12: 19, 63: 19, 131: 19, 34: 33, 37: 44, 38: 18, 42: 48, 52: 48, 58: 45, 79: 48, 113: 3, 116: 1, 119: 44, 129: 48, 30: 0, 33: 0, 66: 0, 76: 0, 7: 29, 15: 33, 35: 31, 39: 33, 49: 31, 106: 29, 122: 29, 100: 0, 6: 0, 5: 38, 17: 45, 19: 38, 29: 40, 51: 52, 72: 0, 88: 36, 89: 38, 97: 0, 104: 0, 105: 45, 110: 13, 16: 1, 24: 1, 32: 1, 83: 1, 123: 1, 3: 50, 23: 50, 45: 46, 46: 46, 50: 1, 61: 46, 78: 50, 103: 46, 107: 50, 117: 35, 125: 50, }
        green_veggies = {'arugula (rocket)', 'bok choy', 'broccoli', 'broccoli rabe (rapini)', 'broccolini', 'collard greens', 'leafy lettuce', 'endive', 'escarole', 'kale',
                         'mesclun', 'mixed greens', 'mustard greens', 'romaine lettuce', 'spinach', 'Swiss chard', 'turnip greens', 'watercress'}
        
        orange_veggies = {'acorn squash', 'bell peppers', 'butternut squash', 'carrots', 'hubbard squash', 'pumpkin', 'red chili peppers', 'sweet red peppers', 'sweet potatoes',
                          'tomatoes', '100% vegetable juice'}
        
        starchy_veggies = {'cassava', ' corn', ' green bananas', 'green lima beans', 'green peas', 'parsnips', 'plantains', 'potatoes white', 'taro', 'water chestnuts', 'yams'}
        product_record = element
        product_name = product_record.get('product_name')
        product_id = product_record.get('product_id')
        aisle_id = product_record.get('aisle_id')
        done = False
        if aisle_id not in food_id_map.keys():
            aisle_id = 24
        
        food_id = food_id_map[aisle_id]
        
        if food_id == 0:
            food_id = 1

        #whole grain vs not
        if food_id == 20:
            if 'whole' in product_name or 'grain' in product_name:
                food_id = 16
                
         #lowfat vs not
        elif food_id in range(25,27):
            if 'low' in product_name or 'skim' in product_name :
                #regular fat and low fat offset by 3 id's
                food_id += 3
            
        #canned vegetable color
        elif food_id == 13:
            if product_name is not None:
                #check for green
                for veg in green_veggies:
                    if veg in product_name or product_name in veg:
                        food_id = 5
                        done=True
                if not done:
                    for veg in starchy_veggies:
                        if veg in product_name or product_name in veg:
                            food_id = 9
                            done=True

                if not done:
                    for veg in orange_veggies:
                        if veg in product_name or product_name in veg:
                            food_id = 7
     
        #fresh or frozen vegetable color
        elif food_id == 1 or food_id == 12:
            if product_name is not None:
                for veg in green_veggies:
                    if veg in product_name or product_name in veg:
                        food_id = 4
                        done=True
                if not done:
                    for veg in starchy_veggies:
                        if veg in product_name or product_name in veg:
                            food_id = 8
                            done=True

                if not done:
                    for veg in orange_veggies:
                        if veg in product_name or product_name in veg:
                            food_id = 6

        #low or reg fat meat
        elif food_id == 29:
            if 'low' in product_name or 'lean' in product_name:
                food_id = 28
                
        # create nom match
        food_map_record = {
            "food_id" : food_id,
            "product_id" : product_id,
        }
        return [food_map_record]
    
def run():
    PROJECT_ID = 'responsive-cab-267123' # change to your project id
    BUCKET = 'gs://bmease_cs327e' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
        # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'foodmap-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    # Create beam pipeline using local runner
    p = Pipeline(options=options)
    
    # get average price per year for each food
    sql = "select lower(product_name) as product_name, product_id, a.aisle_id, department from instacart_modeled.Products p inner join instacart_modeled.Departments d on d.department_id = p.department_id inner join instacart_modeled.Aisles a on a.aisle_id = p.aisle_id where d.department_id not in (5,8,11,17,18) and p.product_name not like '%Filters%' order by product_name"  
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     # write PCollection to input file
    query_results | 'Write to input.txt' >> WriteToText(DIR_PATH + 'input.txt')

     # apply ParDo to format the key, value pairs
        # key is the food_id and value is a tuple of year and average price that year
    nom_match_pcoll = query_results | 'Food and matches from nom' >> beam.ParDo(MatchProductFn())
        
     # write PCollection to output file
    nom_match_pcoll | 'Write to output.txt' >> WriteToText(DIR_PATH + 'output.txt')

    dataset_id = 'USDA_ERS_modeled'
    table_id = 'Food_Map_Beam_DF'
    schema_id = 'food_id:INTEGER,product_id:INTEGER'

     # write PCollection to new BQ table
    nom_match_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
                                                  table=table_id, 
                                                  schema=schema_id,
                                                  project=PROJECT_ID,
                                                  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                  write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                  batch_size=int(100))
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
    