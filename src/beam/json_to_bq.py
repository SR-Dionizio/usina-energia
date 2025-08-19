import logging
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io import ReadAllFromText


class ProcessGCSNotification(beam.DoFn):
    """
    Extrai o caminho do arquivo GCS da notificação do Pub/Sub.
    """

    def process(self, element):
        try:
            data = json.loads(element.decode('utf-8'))
            bucket = data['bucket']
            file_name = data['name']

            yield f'gs://{bucket}/{file_name}'
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f'Falha ao processar a notificação do GCS. Erro: {e}')
            pass


def process_file_content(line):
    """
    Lê o conteúdo de um arquivo e processa cada linha como um registro JSON.
    """
    try:
        record = json.loads(line)
        record['_datetime_insert'] = datetime.now(timezone.utc).isoformat()
        yield record
    except json.JSONDecodeError as e:
        logging.error(f'Falha ao parsear a linha JSON: {line}. Erro: {e}')
        # Em produção: enviar para Dead-Letter Queue
        pass


def run_streaming_pipeline(input_topic, output_table, table_schema, pipeline_args=None):
    """
    Cria e executa o pipeline Apache Beam para ingestão de streaming.
    """
    if pipeline_args is None:
        pipeline_args = []

    # ✅ Define que o pipeline é de streaming
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Lê a notificação do Pub/Sub
        notifications = p | 'Read from PubSub' >> ReadFromPubSub(topic=input_topic)

        # 2. Extrai o caminho do arquivo da notificação
        file_paths = notifications | 'Get File Path' >> beam.ParDo(ProcessGCSNotification())

        # 3. Lê o conteúdo dos arquivos do GCS
        lines = (
            file_paths
            | 'Read GCS File Contents' >> ReadAllFromText()
        )

        # 4. Processa cada linha JSON
        records = lines | 'Process File Content' >> beam.FlatMap(process_file_content)

        # 5. Grava no BigQuery
        records | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    telemetry_schema = {
        'fields': [
            {'name': 'sensor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'equipment_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'unit', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': '_datetime_insert', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        ]
    }

    pipeline_args = [
        '--runner=DataflowRunner',
        '--temp_location=gs://usina-energia-dados/temp',
        '--staging_location=gs://usina-energia-dados/staging',
        '--region=us-central1',
        '--worker_machine_type=e2-standard-2',
        '--project=usina-energia',
        '--streaming'
    ]

    run_streaming_pipeline(
        input_topic='projects/usina-energia/topics/telemetria-topic',
        output_table='usina-energia:raw.telemetria',
        table_schema=telemetry_schema,
        pipeline_args=pipeline_args
    )
