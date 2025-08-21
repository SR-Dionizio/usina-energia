import logging
import re
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches


class ProcessAndAddMetadataJSON(beam.DoFn):
    """
    DoFn que lê o conteúdo de um arquivo JSON ou JSONL,
    transforma em dict(s) e adiciona colunas de metadados.
    """

    def process(self, element, *args, **kwargs):
        file_obj = element
        file_path = file_obj.metadata.path

        # Extrai metadados do caminho do arquivo
        bucket_match = re.search(r'gs://([^/]+)/', file_path)
        source_bucket = bucket_match.group(1) if bucket_match else 'unknown_bucket'
        source_file = file_path.split('/')[-1]

        content = file_obj.read().decode('utf-8').strip()
        if not content:
            return

        records = []
        try:
            # Tenta primeiro como JSON "normal"
            data = json.loads(content)
            if isinstance(data, dict):
                records = [data]
            elif isinstance(data, list):
                records = data
        except json.JSONDecodeError:
            # Se falhar, tenta JSONL (uma linha por objeto)
            try:
                records = [json.loads(line) for line in content.splitlines() if line.strip()]
            except json.JSONDecodeError:
                logging.error(f"Arquivo {source_file} inválido: não é JSON nem JSONL")
                return

        # Adiciona metadados
        for row in records:
            if isinstance(row, dict):
                row["_source_file"] = source_file
                row["_source_storage"] = source_bucket
                row["_datetime_insert"] = datetime.now(timezone.utc).isoformat()
                yield row


def run(input_file_pattern, output_table, table_schema, pipeline_args):
    """
    Cria e executa o pipeline Apache Beam para ingestão de JSON.
    """
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Lendo todos os arquivos e processando
        records = (p
                   | 'Match Files' >> MatchFiles(input_file_pattern)
                   | 'Read Matches' >> ReadMatches()
                   | 'Process JSON and Add Metadata' >> beam.ParDo(ProcessAndAddMetadataJSON()))

        # Carregando no BigQuery
        records | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    table_schema = {
        "fields": [
            {'name': 'sensor_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'equipment_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'unit', 'type': 'STRING', 'mode': 'NULLABLE'},
            {"name": "_source_file", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_source_storage", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_datetime_insert", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
    }

    pipeline_args = [
        '--runner=DirectRunner',
        '--temp_location=gs://usina-energia-dados/temp',
        '--staging_location=gs://usina-energia-dados/staging',
    ]

    run(
        input_file_pattern='gs://usina-energia-dados/usina-energia/telemetria/*.json',
        output_table='usina-energia:raw.telemetria',
        table_schema=table_schema,
        pipeline_args=pipeline_args
    )
