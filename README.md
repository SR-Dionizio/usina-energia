📘 Documentação de Ingestão e Modelagem de Dados

Este repositório contém os pipelines de ingestão e os schemas das tabelas no BigQuery para o projeto Usina Energia.
A arquitetura segue um modelo em camadas: RAW → TRU → REF.

🚀 Pipelines de Ingestão (Apache Beam)
1. csv_to_bq.py

Objetivo: ingestão de arquivos CSV de manutenção do GCS para o BigQuery.

Modo: Batch.

Fonte: gs://usina-energia-dados/usina-energia/manutencao/*.csv

Destino: usina-energia.raw.manutencao

Funcionalidades:

Leitura de CSV com cabeçalho.

Transformação em dict por linha.

Inclusão de metadados:

_source_file

_source_storage

_datetime_insert

Schema (raw.manutencao)
Campo	Tipo	Obrigatório	Descrição
maintenance_id	STRING	✅	Identificador da manutenção
equipment_id	STRING	✅	Identificador do equipamento
maintenance_type	STRING	✅	Tipo de manutenção
description	STRING	❌	Descrição da manutenção
start_date	DATE	❌	Data de início
end_date	DATE	❌	Data de término
downtime_hours	FLOAT	❌	Horas de indisponibilidade
technician	STRING	❌	Técnico responsável
_source_file	STRING	❌	Nome do arquivo de origem
_source_storage	STRING	❌	Bucket de origem
_datetime_insert	TIMESTAMP	❌	Data/hora da ingestão
2. json_to_bq.py

Objetivo: ingestão de arquivos JSON de telemetria via Pub/Sub para o BigQuery.

Modo: Streaming (Dataflow).

Fonte: tópico Pub/Sub → projects/usina-energia/topics/telemetria-topic

Destino: usina-energia.raw.telemetria

Funcionalidades:

Recebe notificação de arquivo do GCS.

Lê e processa JSON linha a linha.

Adiciona campo _datetime_insert.

Schema (raw.telemetria)
Campo	Tipo	Obrigatório	Descrição
sensor_id	STRING	❌	Identificador do sensor
equipment_id	STRING	❌	Identificador do equipamento
timestamp	STRING	❌	Timestamp da leitura
value	STRING	❌	Valor da leitura
unit	STRING	❌	Unidade de medida
_datetime_insert	TIMESTAMP	❌	Data/hora da ingestão
🗄️ Schemas SQLX
1. RAW (ingestão bruta)

raw_manutencao.sqlx → Estrutura da tabela raw.manutencao (alinhada com csv_to_bq.py).

raw_telemetria.sqlx → Estrutura da tabela raw.telemetria (alinhada com json_to_bq.py).

2. TRU (trusted/curado)

tru_manutencao.sqlx → Versão tratada de manutenção.

Normalmente remove duplicatas, valida tipos e datas.

tru_telemetria.sqlx → Versão tratada de telemetria.

Limpeza de registros inválidos, padronização de timestamp.

3. REF (referência)

ref_resumo_telemetria.sqlx → Tabela de acompanhamento e referência de sensores.

Possível uso: enriquecer dados de telemetria com descrições, limites de operação, unidades padrão.

🔄 Fluxo de Dados
flowchart TD
    subgraph GCS[Google Cloud Storage]
        CSV[Arquivos CSV - Manutenção]
        JSON[Arquivos JSON - Telemetria]
    end

    subgraph PubSub[Google Pub/Sub]
        TOPIC[telemetria-topic]
    end

    CSV -->|csv_to_bq.py| RAW_MANUTENCAO[raw.manutencao]
    JSON -->|Notificação| TOPIC -->|json_to_bq.py| RAW_TELEMETRIA[raw.telemetria]

    RAW_MANUTENCAO --> TRU_MANUTENCAO[tru.manutencao]
    RAW_TELEMETRIA --> TRU_TELEMETRIA[tru.telemetria]

    TRU_TELEMETRIA --> REF_TELEMETRIA[ref.acompenhamento_telemetria]

📌 Observações

Pipelines de ingestão populam apenas a camada RAW.

As tabelas TRU e REF devem ser povoadas via transformações SQL/ELT.

_datetime_insert garante rastreabilidade de carga.

_source_file e _source_storage permitem auditoria e troubleshooting.

