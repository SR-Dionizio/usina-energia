import logging
import json
import re
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def add_metadata_and_parse(element):
    """
    Função que adiciona metadados e converte a string JSON em um dicionário.
    """
    line, file_path = element

    # Extrai metadados do caminho do arquivo
    bucket_match = re.search(r'gs://([^/]+)/', file_path)
    source_bucket = bucket_match.group(1) if bucket_match else 'unknown_bucket'
    source_file = file_path.split('/')[-1]

    # Converte a linha JSON em um dicionário
    try:
        record = json.loads(line)
    except json.JSONDecodeError as e:
        logging.error(f'Falha ao parsear a linha JSON: {line}. Erro: {e}')
        return None  # Retorna None para descartar o registro inválido

    # Adiciona os metadados
    record['_source_file'] = source_file
    record['_source_storage'] = source_bucket
    record['_datetime_insert'] = datetime.now(timezone.utc).isoformat()

    return record


def run(input_file_pattern, output_table, pipeline_args=None):
    """
    Cria e executa o pipeline Apache Beam para ingestão de JSON.

    Args:
        input_file_pattern (str): Caminho do(s) arquivo(s) de entrada no GCS.
        output_table (str): Nome completo da tabela de destino no BigQuery.
        pipeline_args (list): Argumentos para a execução do pipeline.
    """
    if pipeline_args is None:
        pipeline_args = []

    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Combina a leitura dos arquivos com o nome do arquivo de origem
        lines_and_paths = (p | 'Match Files' >> beam.io.fileio.MatchFiles(input_file_pattern)
                           | 'Read File Contents' >> beam.io.fileio.ReadMatches()
                           | 'Split into Lines' >> beam.FlatMap(
                    lambda file_obj: [(line, file_obj.path) for line in file_obj.read().decode('utf-8').splitlines()]))

        # Adiciona metadados e parseia o JSON
        records = lines_and_paths | 'Add Metadata & Parse JSON' >> beam.Map(add_metadata_and_parse)

        # Filtra os registros inválidos que retornaram None
        valid_records = records | 'Filter Invalid Records' >> beam.Filter(lambda x: x is not None)

        # Carrega os dicionários no BigQuery
        valid_records | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            schema='SCHEMA_AUTODETECT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    # Exemplo de como chamar a função com argumentos fixos
    # Para uso no Airflow, você passaria os parâmetros dinamicamente.
    # pipeline_args = [
    #     '--runner=DirectRunner', # ou DataflowRunner
    #     '--temp_location=gs://seu-bucket/temp',
    #     '--staging_location=gs://seu-bucket/staging',
    # ]
    # run(
    #     input_file_pattern='gs://seu-bucket/arquivos/json/*.json',
    #     output_table='seu-projeto:seu_dataset.tabela_json',
    #     pipeline_args=pipeline_args
    # )