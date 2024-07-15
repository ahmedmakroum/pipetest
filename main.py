import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromText
from apache_beam.io.textio import ReadAllFromText
import csv
import json

class ParseCSV(beam.DoFn):
    def process(self, element):
        for row in csv.DictReader([element]):
            yield row

class FilterAPICalls(beam.DoFn):
    def process(self, element):
        cs_uri = element['cs_uri']
        if not cs_uri.startswith('/assets') and not cs_uri.startswith('/admin'):
            yield element

class CalculateTraffic(beam.DoFn):
    def process(self, element):
        element['total_bytes'] = int(element['cs_bytes']) + int(element['sc_bytes'])
        yield element

class AggregateMetrics(beam.DoFn):
    def process(self, element):
        key, values = element
        total_calls = len(values)
        total_traffic = sum(int(value['total_bytes']) for value in values) / (1024 * 1024 * 1024)  # Convert to GB
        
        pricing = 0
        if total_calls > 500000000:
            pricing = (total_calls - 500000000) / 1000000 * 13 + 4500000 + 800000
        elif total_calls > 50000000:
            pricing = (total_calls - 50000000) / 1000000 * 16 + 1000000
        else:
            pricing = total_calls / 1000000 * 20
        
        egress_cost = total_traffic * 0.1
        total_cost = pricing + egress_cost
        
        yield {
            'month': key,
            'total_calls': total_calls,
            'total_traffic_gb': total_traffic,
            'api_call_cost': pricing,
            'egress_cost': egress_cost,
            'total_cost': total_cost
        }

def run():
    pipeline_options = PipelineOptions()
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = 'arcane-boulder-429415-s1'
    gcp_options.job_name = 'api-traffic-cost-calculation'
    gcp_options.staging_location = 'gs://bucket1/staging'
    gcp_options.temp_location = 'gs://bucket1/temp'
    pipeline_options.view_as(PipelineOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'Read CSV files' >> ReadAllFromText('gs://bucketboti1/bucket1.csv')
            | 'Parse CSV' >> beam.ParDo(ParseCSV())
            | 'Filter API calls' >> beam.ParDo(FilterAPICalls())
            | 'Calculate Traffic' >> beam.ParDo(CalculateTraffic())
            | 'Add Month Key' >> beam.Map(lambda x: (x['time_micros'][:6], x))
            | 'Group by Month' >> beam.GroupByKey()
            | 'Aggregate Metrics' >> beam.ParDo(AggregateMetrics())
            | 'Write to BigQuery' >> WriteToBigQuery(
                table='arcane-boulder-429415-s1:test',
                schema='month:STRING, total_calls:INTEGER, total_traffic_gb:FLOAT, api_call_cost:FLOAT, egress_cost:FLOAT, total_cost:FLOAT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run()
