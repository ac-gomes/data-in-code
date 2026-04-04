# Databricks notebook source
# MAGIC %md
# MAGIC # Vendas Base Ingestion
# MAGIC
# MAGIC **Feature**: vendas_base_ingestion  
# MAGIC **Versão**: 1.0.0  
# MAGIC **Descrição**: Ingestão de dados da aba "Base" do arquivo Excel para tabela Delta
# MAGIC
# MAGIC ## Arquitetura
# MAGIC ```
# MAGIC Excel (aba Base) → Transformações → Delta Table (vendas_base)
# MAGIC ```
# MAGIC
# MAGIC ## Dependências
# MAGIC * pandas
# MAGIC * openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações

# COMMAND ----------

# DBTITLE 1,Setup de Logging Header
# MAGIC %md
# MAGIC ## 1.1 Setup de Logging

# COMMAND ----------

# DBTITLE 1,Import LogControl
# MAGIC %run ../../error_handler_logging/src/logger_control

# COMMAND ----------

# DBTITLE 1,Configure LogControl
# Configurar LogControl
logger = LogControl(
    logger_name="vendas_base_ingestion",
    tbl_name="main.vendas_regionais.tb_logs_ingestion"
)

logger.log_info("=== INICIANDO VENDAS BASE INGESTION ===")
logger.log_info(f"Arquivo origem: {FILE_PATH}")
logger.log_info(f"Aba: {SHEET_NAME}")
logger.log_info(f"Destino: {FULL_TABLE_NAME}")
logger.log_info(f"Modo: {WRITE_MODE}")

# COMMAND ----------

# DBTITLE 1,Configurações
# Parâmetros de configuração
FILE_PATH = "/Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/arquivos/VendasRegionaisVBA.xlsm"
SHEET_NAME = "Base"

# Configuração do destino
TARGET_CATALOG = "hive_metastore"  # Alterar para seu catálogo UC se necessário
TARGET_SCHEMA = "vendas_regionais"
TARGET_TABLE = "vendas_base"
FULL_TABLE_NAME = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

# Modo de escrita
WRITE_MODE = "overwrite"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura do Arquivo Excel

# COMMAND ----------

# DBTITLE 1,Leitura do Arquivo Excel
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

try:
    logger.log_info("Iniciando leitura do arquivo Excel")
    df_raw = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME, engine='openpyxl')
    logger.log_success(f"Arquivo lido: {len(df_raw)} registros, {df_raw.shape[1]} colunas")
    logger.log_info(f"Colunas: {', '.join(df_raw.columns.tolist())}")
except Exception as e:
    logger.log_error("Erro ao ler arquivo Excel")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza de Dados

# COMMAND ----------

# DBTITLE 1,Limpeza de Dados
import re
from unidecode import unidecode

def clean_column_name(col_name):
    """Converte nome de coluna para snake_case"""
    # Remover acentos
    col_name = unidecode(str(col_name))
    # Converter para minúsculas
    col_name = col_name.lower()
    # Substituir espaços e caracteres especiais por underscore
    col_name = re.sub(r'[^a-z0-9]+', '_', col_name)
    # Remover underscores no início e fim
    col_name = col_name.strip('_')
    return col_name

# Limpar dados
logger.log_info("Iniciando limpeza de dados")

# Remover colunas vazias (Unnamed)
cols_to_keep = [col for col in df_raw.columns if not col.startswith('Unnamed')]
df_clean = df_raw[cols_to_keep].copy()

# Remover linhas completamente vazias
df_clean = df_clean.dropna(how='all')

# Renomear colunas para snake_case
column_mapping = {
    'Data da Venda': 'data_venda',
    'Região': 'regiao',
    'Vendedor': 'vendedor',
    'Código Vendedor': 'codigo_vendedor',
    'Seção': 'secao',
    'Vendas': 'valor_vendas',
    'Mês': 'mes_abrev'
}

df_clean = df_clean.rename(columns=column_mapping)

# Trim de strings
string_cols = df_clean.select_dtypes(include=['object']).columns
for col in string_cols:
    df_clean[col] = df_clean[col].astype(str).str.strip()

logger.log_success(f"Limpeza concluída: {len(df_clean)} registros, colunas: {list(df_clean.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Enriquecimento de Dados

# COMMAND ----------

# DBTITLE 1,Enriquecimento de Dados
logger.log_info("Iniciando enriquecimento de dados")

# Garantir que data_venda é datetime
df_clean['data_venda'] = pd.to_datetime(df_clean['data_venda'])

# Extrair ano e mês
df_clean['ano'] = df_clean['data_venda'].dt.year
df_clean['mes'] = df_clean['data_venda'].dt.month

# Adicionar timestamp de carga
df_clean['data_carga'] = datetime.now()

logger.log_success(f"Enriquecimento concluído: adicionadas colunas ano, mes, data_carga")
logger.log_info(f"Schema final: {df_clean.dtypes.to_dict()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validações de Qualidade

# COMMAND ----------

# DBTITLE 1,Validações de Qualidade
logger.log_info("Executando validações de qualidade")

# Validação 1: Valores nulos
null_counts = df_clean.isnull().sum()
if null_counts.sum() > 0:
    logger.log_warning(f"Valores nulos encontrados: {null_counts[null_counts > 0].to_dict()}")
else:
    logger.log_info("Validação 1/5: Nenhum valor nulo encontrado")

# Validação 2: Range de codigo_vendedor
invalid_codes = df_clean[~df_clean['codigo_vendedor'].between(1, 8)]
if len(invalid_codes) > 0:
    logger.log_warning(f"Validação 2/5: {len(invalid_codes)} códigos de vendedor inválidos (fora do range 1-8)")
else:
    logger.log_info("Validação 2/5: Todos os códigos de vendedor estão no range 1-8")

# Validação 3: Regiões válidas
valid_regions = ['Norte', 'Sul', 'Nordeste', 'Sudeste']
invalid_regions = df_clean[~df_clean['regiao'].isin(valid_regions)]
if len(invalid_regions) > 0:
    logger.log_warning(f"Validação 3/5: {len(invalid_regions)} regiões inválidas encontradas")
else:
    logger.log_info("Validação 3/5: Todas as regiões são válidas")

# Validação 4: Valores de vendas positivos
negative_sales = df_clean[df_clean['valor_vendas'] <= 0]
if len(negative_sales) > 0:
    logger.log_warning(f"Validação 4/5: {len(negative_sales)} vendas não-positivas encontradas")
else:
    logger.log_info("Validação 4/5: Todos os valores de vendas são positivos")

# Validação 5: Duplicatas
duplicates = df_clean.duplicated(subset=['data_venda', 'vendedor', 'secao'], keep=False)
dup_count = duplicates.sum()
if dup_count > 0:
    logger.log_warning(f"Validação 5/5: {dup_count} registros duplicados encontrados")
else:
    logger.log_info("Validação 5/5: Nenhuma duplicata encontrada")

logger.log_success("Validações de qualidade concluídas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Visualização dos Dados

# COMMAND ----------

# DBTITLE 1,Visualização dos Dados
logger.log_info("Exibindo amostra dos dados transformados")
display(df_clean.head(10))

logger.log_info("Exibindo estatísticas descritivas")
display(df_clean.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conversão para Spark DataFrame

# COMMAND ----------

# DBTITLE 1,Conversão para Spark DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

logger.log_info("Convertendo pandas DataFrame para Spark DataFrame")

# Definir schema explícito
schema = StructType([
    StructField("data_venda", DateType(), False),
    StructField("regiao", StringType(), False),
    StructField("vendedor", StringType(), False),
    StructField("codigo_vendedor", IntegerType(), False),
    StructField("secao", StringType(), False),
    StructField("valor_vendas", DecimalType(10, 2), False),
    StructField("mes_abrev", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("mes", IntegerType(), False),
    StructField("data_carga", TimestampType(), False)
])

# Converter pandas para Spark DataFrame
spark_df = spark.createDataFrame(df_clean, schema=schema)

logger.log_success(f"Conversão concluída: {spark_df.count()} registros")
logger.log_info(f"Schema Spark: {spark_df.schema.simpleString()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criar Schema (se não existir)

# COMMAND ----------

# DBTITLE 1,Criar Schema
logger.log_info(f"Verificando/criando schema {TARGET_SCHEMA}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

logger.log_success(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} verificado/criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Escrita na Tabela Delta

# COMMAND ----------

# DBTITLE 1,Escrita na Tabela Delta
try:
    logger.log_info(f"Iniciando escrita na tabela {FULL_TABLE_NAME} (modo: {WRITE_MODE})")
    
    # Escrever tabela Delta
    spark_df.write \
        .format("delta") \
        .mode(WRITE_MODE) \
        .option("overwriteSchema", "true") \
        .saveAsTable(FULL_TABLE_NAME)
    
    logger.log_success(f"Tabela {FULL_TABLE_NAME} criada/atualizada com sucesso")
except Exception as e:
    logger.log_error(f"Erro ao escrever tabela {FULL_TABLE_NAME}")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validações Pós-Carga

# COMMAND ----------

# DBTITLE 1,Validações Pós-Carga
logger.log_info("Executando validações pós-carga")

# Ler a tabela recém-criada
df_delta = spark.table(FULL_TABLE_NAME)

record_count = df_delta.count()
logger.log_info(f"Contagem de registros na tabela: {record_count}")

logger.log_info("Schema da tabela Delta:")
df_delta.printSchema()

logger.log_info("Exibindo primeiros registros da tabela")
display(df_delta.limit(10))

logger.log_info("Exibindo agregações de sanidade por região")
display(
    df_delta.groupBy("regiao") \
        .agg(
            F.count("*").alias("qtd_vendas"),
            F.sum("valor_vendas").alias("total_vendas"),
            F.avg("valor_vendas").alias("ticket_medio")
        ) \
        .orderBy(F.desc("total_vendas"))
)

logger.log_success("Validações pós-carga concluídas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Métricas Finais

# COMMAND ----------

# DBTITLE 1,Métricas Finais
logger.log_info("="*80)
logger.log_info("RESUMO DA INGESTÃO")
logger.log_info("="*80)
logger.log_info(f"Arquivo origem: {FILE_PATH}")
logger.log_info(f"Aba: {SHEET_NAME}")
logger.log_info(f"Tabela destino: {FULL_TABLE_NAME}")
logger.log_info(f"Modo de escrita: {WRITE_MODE}")
logger.log_info(f"Registros processados: {len(df_clean)}")
logger.log_info(f"Registros na tabela: {record_count}")
logger.log_info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
logger.log_info("="*80)
logger.log_success("=== INGESTÃO CONCLUÍDA COM SUCESSO ===")
