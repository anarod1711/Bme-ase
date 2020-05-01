import datetime,logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FoodPricesFn(beam.DoFn):
    def process(self, element):
    # get necessary fields from record
        price_record = element
        food_id = price_record.get('food_id')
        year = price_record.get('year')
        avg_price = price_record.get('avg_price')
        
        #generate key value pairs
        year_price_tuple = (year, avg_price) # value=yearly prices
        food_price_tuple = (food_id, year_price_tuple) 
        return [food_price_tuple]

# predict 2017 price using linear regression
class LinearRegFn(beam.DoFn):
    def process(self,element):
        food_id, price_obj = element # product_obj is an _UnwindowedValues type
        price_list = list(price_obj) # item format :tuple (year=x, price=y)
        
        xs = [] # year
        ys = [] # price
        
        # get x and y values
        for yr_price in price_list:
            xs.append(yr_price[0]) 
            ys.append(yr_price[1])
        
        # least squares fit for y=mx+b over all points
        # src: https://beam.apache.org/releases/pydoc/2.9.0/_modules/apache_beam/transforms/util.html
        n = float(len(xs))
        xbar = sum(xs) / n
        ybar = sum(ys) / n
        m = sum([(x - xbar) * (y - ybar) for x, y in zip(xs, ys)]) / sum([(x - xbar)**2 for x in xs])
        b = ybar - m * xbar
        
        # calculate 2017 price
        year = 2017
        price = m * year + b
        
        # create food price record
        food_record = {
            "food_id" : food_id,
            "year" : year,
            "avg_price" : price
        }
        return [food_record]
    
def run():
    PROJECT_ID = 'responsive-cab-267123' # change to your project id
    BUCKET = 'gs://bmease_cs327e' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-foodmarket-df1',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DataflowRunner', options=opts)
    
     # get average price per year for each food
    sql = 'SELECT food_id, year, AVG(price) as avg_price FROM USDA_ERS_modeled.Food_Market WHERE price IS NOT NULL and year IS NOT NULL GROUP BY food_id, year ORDER BY food_id'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
     # write PCollection to input file
    query_results | 'Write to input.txt' >> WriteToText('input.txt')

     # apply ParDo to format the key, value pairs
        # key is the food_id and value is a tuple of year and average price that year
    y_price_pcoll = query_results | 'Food and average price per year pairs' >> beam.ParDo(FoodPricesFn())
        
     # write PCollection to output file
    y_price_pcoll | 'Write to output.txt' >> WriteToText('output.txt')
        
     # group yearly prices by food_id
    grouped_food_yprices_pcoll =  y_price_pcoll | 'Group by food_id' >> beam.GroupByKey()
    
     # write PCollection to log file
    grouped_food_yprices_pcoll | 'Write log 3' >> WriteToText('grouped_food_yprices_pcoll.txt')
    
    # predict 2017 price using linear regression
    food_2017_prices_pcoll = grouped_food_yprices_pcoll | 'Predict 2017 prices' >> beam.ParDo(LinearRegFn())
    
    # write PCollection to log file
    food_2017_prices_pcoll | 'Write log 4' >> WriteToText('food_2017_prices_pcoll.txt')
    
    # merge average yearly prices
    food_yr_price = (food_2017_prices_pcoll, query_results) | 'Merge PCollections' >> beam.Flatten()
    
    # write PCollection to log file
    food_yr_price | 'Write Merged Collections' >> WriteToText('merged.txt')

    dataset_id = 'USDA_ERS_modeled'
    table_id = 'Food_Market_Beam_DF'
    schema_id = 'food_id:INTEGER,year:INTEGER,avg_price:FLOAT'

     # write PCollection to new BQ table
    food_yr_price | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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