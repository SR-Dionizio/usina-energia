import logging
import re
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches


class ProcessAndAddMetadata(beam.DoFn):
    """
    DoFn que lê o conteúdo de um arquivo CSV,
    transforma cada linha em dict usando o cabeçalho
    e adiciona colunas de metadados.
    """

    def process(self, element, *args, **kwargs):
        file_obj = element
        file_path = file_obj.metadata.path

        # Extrai metadados do caminho do arquivo
        bucket_match = re.search(r'gs://([^/]+)/', file_path)
        source_bucket = bucket_match.group(1) if bucket_match else 'unknown_bucket'
        source_file = file_path.split('/')[-1]

        # Lê o conteúdo completo do arquivo e divide em linhas
        content = file_obj.read().decode('utf-8')
        lines = content.splitlines()

        if not lines:
            return

        # Cabeçalho do CSV
        header = lines[0].split(',')

        # Processa cada linha de dados
        for line in lines[1:]:
            values = line.split(',')
            row = dict(zip(header, values))

            # Adiciona metadados
            row["_source_file"] = source_file
            row["_source_storage"] = source_bucket
            row["_datetime_insert"] = datetime.now(timezone.utc).isoformat()

            yield row


def run(input_file_pattern, output_table, table_schema, pipeline_args):
    """
    Cria e executa o pipeline Apache Beam para ingestão de CSV.

    Args:
        input_file_pattern (str): Caminho do(s) arquivo(s) de entrada no GCS.
        output_table (str): Nome completo da tabela de destino no BigQuery.
        table_schema (dict): Schema da tabela do BigQuery.
        pipeline_args (list): Argumentos para a execução do pipeline.
    """
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Lendo todos os arquivos e processando em batch
        records = (p
                   | 'Match Files' >> MatchFiles(input_file_pattern)
                   | 'Read Matches' >> ReadMatches()
                   | 'Process Files and Add Metadata' >> beam.ParDo(ProcessAndAddMetadata()))

        # Carregando os dados no BigQuery
        records | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    # Exemplo de schema para o CSV de manutenção
    table_schema = {
        "fields": [
            {"name": "maintenance_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "equipment_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "maintenance_type", "type": "STRING", "mode": "REQUIRED"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "start_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "end_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "downtime_hours", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "technician", "type": "STRING", "mode": "NULLABLE"},
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
        input_file_pattern='gs://usina-energia-dados/usina-energia/manutencao/*.csv',
        output_table='usina-energia:raw.manutencao',
        table_schema=table_schema,
        pipeline_args=pipeline_args
    )
