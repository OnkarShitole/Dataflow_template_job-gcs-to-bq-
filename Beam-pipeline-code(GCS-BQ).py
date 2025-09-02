import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from datetime import datetime
import csv

# Pipeline options
options = PipelineOptions()
gcp_options = options.view_as(GoogleCloudOptions)
gcp_options.project = 'centered-sol-469812-v8'
gcp_options.region = 'us-central1'
gcp_options.job_name = 'gcs-to-bq-pipeline'
gcp_options.temp_location = 'gs://my-bucket-88/temp'

options.view_as(StandardOptions).runner = 'DataflowRunner'
options.view_as(StandardOptions).streaming = False

# Input/Output
gcs_input = 'gs://my-bucket-88/sales.csv'
bq_output = 'centered-sol-469812-v8.TAsk_1.sales_data'

# Function to parse CSV rows into dicts for BigQuery
def parse_csv(line):
    # Split the line by comma and handle potential empty values
    fields = line.strip().split(',')
    
    # Check for correct number of fields to avoid index errors
    if len(fields) != 4:
        return {} # Return an empty dict or handle the error
        
    sale_date_str = fields[1]
    
    try:
        # Convert types and format for BigQuery
        return {
            'sale_id': int(fields[0]),
            'sale_date': datetime.strptime(sale_date_str, '%Y-%m-%d').date(), # Parse and get date object
            'amount': int(fields[2]),
            'customer_id': int(fields[3])
        }
    except (ValueError, IndexError):
        # Handle parsing errors gracefully
        return {}
        
# Beam pipeline
with beam.Pipeline(options=options) as p:
    (p
     | 'ReadFromGCS' >> beam.io.ReadFromText(gcs_input, skip_header_lines=1)
     | 'ParseCSV' >> beam.Map(parse_csv)
     | 'FilterEmptyLines' >> beam.Filter(lambda d: d) # Filter out empty dictionaries from failed parsing
     | 'WriteToBQ' >> beam.io.WriteToBigQuery(
         bq_output,
         schema='sale_id:INTEGER, sale_date:DATE, amount:INTEGER, customer_id:INTEGER',
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
       )
    )
    