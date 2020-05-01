import datetime,logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class OrdersThatIncludeFn(beam.DoFn):
    def process(self, element):
    # get necessary fields from record
        order_record = element
        prod_id = order_record.get('product_id')
        product_tuple = (prod_id, 1)
        return [product_tuple]

class OrderTotalCalculationsFn(beam.DoFn):
    def process(self,element):
        product_id, product_obj = element # product_obj is an _UnwindowedValues type
        product_list = list(product_obj)
        freq=len(product_list)
            
        product_record = {
            "product_id" : product_id,
            "frequency" : freq
        }
        return [product_record]
    
def run():
    PROJECT_ID = 'responsive-cab-267123' # change to your project id
    BUCKET = 'gs://bmease_cs327e' # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'orders-df1',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DataflowRunner', options=opts)
    
    sql = 'SELECT product_id FROM instacart_modeled.Order_Products'    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     # write PCollection to input file
    query_results | 'Write to input.txt' >> WriteToText(DIR_PATH + 'input.txt')

     # apply ParDo to format the order's day of the week
    orders_that_include_pcoll = query_results | 'Count Orders' >> beam.ParDo(OrdersThatIncludeFn())
        
     # write PCollection to output file
    orders_that_include_pcoll | 'Write to output.txt' >> WriteToText(DIR_PATH + 'output.txt')
        
     # group orders by product number
    grouped_orders_pcoll =  orders_that_include_pcoll | 'Group by product_id' >> beam.GroupByKey()
    
     # write PCollection to log file
    grouped_orders_pcoll | 'Write log 3' >> WriteToText(DIR_PATH + 'grouped_orders_pcoll.txt')
    
        # remove duplicate product records
    distinct_orders_pcoll = grouped_orders_pcoll | 'Count product counts and frequencies' >> beam.ParDo(OrderTotalCalculationsFn())
    
    # write PCollection to log file
    distinct_orders_pcoll | 'Write log 4' >> WriteToText(DIR_PATH + 'distinct_products_pcoll.txt')

    dataset_id = 'instacart_modeled'
    table_id = 'Order_Products_Beam_DF'
    schema_id = 'product_id:INTEGER,frequency:INTEGER'

     # write PCollection to new BQ table
    distinct_orders_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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