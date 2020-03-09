import datetime,logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

class FormatDOWFn(beam.DoFn):
  def process(self, element):
    # get necessary fields from record
    order_record = element
    order_dow = order_record.get('order_dow')
    print('current dow: ' + str(order_dow))

    # reformat dow with
    #   0   =   sunday
    #   1   =   monday
    #   2   =   tuesday etc.
    days=['sunday',
    'monday',
    'tuesday',
    'wednesday',
    'thursday',
    'friday',
    'saturday']
    dow=days[int(order_dow)]
    print('new dow: ' + str(dow))
    order_record['order_dow'] = dow
    
    return [order_record]
           
def run():
    PROJECT_ID = 'responsive-cab-267123' # change to your project id
    BUCKET = 'gs://bmease_cs327e'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    # run pipeline on Dataflow 
    options = {
        'runner': 'DataflowRunner',
        'job_name': 'transform-orders-df',
        'project': PROJECT_ID,
        'temp_location': BUCKET + '/temp',
        'staging_location': BUCKET + '/staging',
        'machine_type': 'n1-standard-4', # https://cloud.google.com/compute/docs/machine-types
        'num_workers': 1
    }

    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline('DataflowRunner', options=opts)
    
    sql = 'SELECT * FROM instacart_modeled.Orders'
    
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)
    
    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
    
    # write PCollection to input file
    query_results | 'Write to input.txt' >> WriteToText('orders_df_input.txt')
    
    # apply ParDo to format the order's day of the week
    formatted_dow_pcoll = query_results | 'Format DOW' >> beam.ParDo(FormatDOWFn())
    
    # write PCollection to output file
    formatted_dow_pcoll | 'Write to output.txt' >> WriteToText('orders_df_output.txt')
    
    dataset_id = 'instacart_modeled'
    table_id = 'Orders_Beam_DF'
    schema_id = 'order_id:INTEGER,user_id:INTEGER,order_number:INTEGER,order_dow:STRING,order_hour_of_day:INTEGER,days_since_prior_order:INTEGER'
    
    # write PCollection to new BQ table
    formatted_dow_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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