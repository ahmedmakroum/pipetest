import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io import ReadFromText, WriteToBigQuery

class ParseCSV(beam.DoFn):
    def process(self, element):
        import csv
        from io import StringIO

        # Parse the CSV line
        for row in csv.DictReader(StringIO(element)):
            yield {
                'time_micros': int(row['time_micros']),
                'c_ip': row['c_ip'],
                'c_ip_type': int(row['c_ip_type']),
                'c_ip_region': row['c_ip_region'],
                'cs_method': row['cs_method'],
                'cs_uri': row['cs_uri'],
                'sc_status': int(row['sc_status']),
                'cs_bytes': int(row['cs_bytes']),
                'sc_bytes': int(row['sc_bytes']),
                'time_taken_micros': int(row['time_taken_micros']),
                'cs_host': row['cs_host'],
                'cs_referer': row['cs_referer'],
                'cs_user_agent': row['cs_user_agent'],
                's_request_id': row['s_request_id'],
                'cs_operation': row['cs_operation'],
                'cs_bucket': row['cs_bucket'],
                'cs_object': row['cs_object'],
            }

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'arcane-boulder-429415-s1'
    google_cloud_options.job_name = 'csvtobq'
    google_cloud_options.staging_location = 'gs://bucket1/staging'
    google_cloud_options.temp_location = 'gs://bucket1/temp'
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        (p
         | 'Read from GCS' >> ReadFromText('gs://bucketboti1/bucket1.csv', skip_header_lines=1)
         | 'Parse CSV' >> beam.ParDo(ParseCSV())
         | 'Write to BigQuery' >> WriteToBigQuery(
             'test',
             schema='time_micros:INTEGER, c_ip:STRING, c_ip_type:INTEGER, c_ip_region:STRING, cs_method:STRING, cs_uri:STRING, sc_status:INTEGER, cs_bytes:INTEGER, sc_bytes:INTEGER, time_taken_micros:INTEGER, cs_host:STRING, cs_referer:STRING, cs_user_agent:STRING, s_request_id:STRING, cs_operation:STRING, cs_bucket:STRING, cs_object:STRING',
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

if __name__ == '__main__':
    run()
