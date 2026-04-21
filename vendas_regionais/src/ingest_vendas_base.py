# Databricks notebook source
# DBTITLE 1,Vendas Base Ingestion - Header
# MAGIC %md
# MAGIC    
# MAGIC # Vendas Base Ingestion
# MAGIC
# MAGIC **Feature**: vendas_base_ingestion  
# MAGIC **Versão**: 3.1.0  
# MAGIC **Descrição**: Ingestão distribuída de dados sintéticos da pasta `data/` usando PySpark
# MAGIC
# MAGIC ## Arquitetura
# MAGIC ```
# MAGIC data/ (CSV sintéticos) → PySpark (processamento distribuído) → Delta Table
# MAGIC ```
# MAGIC
# MAGIC ## Fluxo Simplificado (v3.1.0)
# MAGIC ```
# MAGIC 1. Ler CSV da pasta data/
# MAGIC 2. Converter imediatamente para PySpark DataFrame
# MAGIC 3. Tipagem básica e derivações (ano, mês, data_carga)
# MAGIC 4. Escrever diretamente na tabela Delta (Unity Catalog)
# MAGIC 5. Validações pós-carga lendo da tabela
# MAGIC ```
# MAGIC
# MAGIC ## Dependências
# MAGIC * PySpark (processamento distribuído)
# MAGIC * pandas (apenas para leitura inicial de CSV)
# MAGIC * Unity Catalog (workspace.vendas_regionais)
# MAGIC
# MAGIC ## Mudança Arquitetural (v3.1.0)
# MAGIC * ✅ **Fluxo direto para tabela** - sem expor DataFrame
# MAGIC * ✅ **PySpark desde o início** - processamento distribuído
# MAGIC * ✅ **Validações da tabela** - evita duplicação de dados
# MAGIC * ✅ **Escalável** - suporta grandes volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurações

# COMMAND ----------

# DBTITLE 1,Setup de Logging Header
# MAGIC %md
# MAGIC ## 1.1 Setup de Logging

# COMMAND ----------

# DBTITLE 1,Import LogControl
# MAGIC %run ./logger_control

# COMMAND ----------

# DBTITLE 1,Configure LogControl
# Configurar LogControl
logger = LogControl(
    logger_name="vendas_base_ingestion",
    tbl_name="main.vendas_regionais.tb_logs_ingestion"
)

logger.log_info("="*80)
logger.log_info("=== INICIANDO VENDAS BASE INGESTION ===")
logger.log_info(f"Fonte: Pasta data/ (dados sintéticos)")
logger.log_info("="*80)

# COMMAND ----------

# DBTITLE 1,Configurações
# Parâmetros de configuração
# Path relativo ao notebook (em src/, subir 1 nível para vendas_regionais/, depois data/)
DATA_DIR = "../data"

# Configuração do destino - Unity Catalog
TARGET_CATALOG = "workspace"  # Unity Catalog (catálogo do workspace)
TARGET_SCHEMA = "vendas_regionais"
TARGET_TABLE = "vendas_base"
FULL_TABLE_NAME = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

# Modo de escrita
WRITE_MODE = "overwrite"

# COMMAND ----------

# DBTITLE 1,Leitura dos Arquivos CSV
# MAGIC %md
# MAGIC ## 2. Leitura dos Arquivos CSV da Pasta data/

# COMMAND ----------

# DBTITLE 1,Leitura dos Arquivos da Pasta data/
from pyspark.sql import functions as F
import os
import pandas as pd

try:
    logger.log_info("="*80)
    logger.log_info(f"Iniciando leitura distribuída dos arquivos CSV da pasta: {DATA_DIR}")
    logger.log_info("="*80)
    
    # Construir path absoluto para o diretório de dados
    # Notebook está em: .../vendas_regionais/src/
    # Dados estão em: .../vendas_regionais/data/
    workspace_base = f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/data-in-code/vendas_regionais"
    data_dir_abs = f"{workspace_base}/data"
    
    # Listar arquivos da pasta (workspace filesystem)
    arquivos = os.listdir(data_dir_abs)
    arquivos_csv = [f for f in arquivos if f.endswith('.csv')]
    
    if not arquivos_csv:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em {data_dir_abs}")
    
    logger.log_info(f"Arquivos CSV encontrados: {len(arquivos_csv)}")
    for arquivo in arquivos_csv:
        logger.log_info(f"  - {arquivo}")
    
    # Ler arquivos CSV e converter imediatamente para Spark DataFrame
    dfs_spark = []
    for arquivo in arquivos_csv:
        csv_path = os.path.join(data_dir_abs, arquivo)
        # Ler com pandas
        df_temp_pandas = pd.read_csv(csv_path)
        # Converter imediatamente para Spark
        df_temp_spark = spark.createDataFrame(df_temp_pandas)
        dfs_spark.append(df_temp_spark)
        logger.log_info(f"  ✓ Lido e convertido: {arquivo} ({len(df_temp_pandas)} registros)")
    
    # Union de todos os Spark DataFrames
    df_raw = dfs_spark[0]
    for df in dfs_spark[1:]:
        df_raw = df_raw.union(df)
    
    record_count = df_raw.count()
    column_count = len(df_raw.columns)
    
    logger.log_success(f"="*80)
    logger.log_success(f"Leitura concluída: {record_count} registros totais, {column_count} colunas")
    logger.log_success(f"="*80)
    logger.log_info(f"Colunas: {', '.join(df_raw.columns)}")
    
except Exception as e:
    logger.log_error("Erro ao ler arquivos CSV")
    logger.error_handler(e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza de Dados

# COMMAND ----------

# DBTITLE 1,Limpeza de Dados
from pyspark.sql import functions as F

logger.log_info("Iniciando limpeza de dados")

# Remover colunas vazias (Unnamed) - filtrar colunas que não começam com 'Unnamed'
cols_to_keep = [col for col in df_raw.columns if not col.startswith('Unnamed')]
df_clean = df_raw.select(cols_to_keep)

# Remover linhas completamente vazias (pelo menos uma coluna não-nula)
filter_condition = None
for col in cols_to_keep:
    if filter_condition is None:
        filter_condition = F.col(col).isNotNull()
    else:
        filter_condition = filter_condition | F.col(col).isNotNull()

df_clean = df_clean.filter(filter_condition)

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

for old_name, new_name in column_mapping.items():
    if old_name in df_clean.columns:
        df_clean = df_clean.withColumnRenamed(old_name, new_name)

# Trim de strings em todas as colunas do tipo string
for col_name in df_clean.columns:
    col_type = dict(df_clean.dtypes)[col_name]
    if col_type == 'string':
        df_clean = df_clean.withColumn(col_name, F.trim(F.col(col_name)))

logger.log_success(f"Limpeza concluída: {df_clean.count()} registros, colunas: {df_clean.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC    
# MAGIC ## 4. Tipagem e Derivações

# COMMAND ----------

# DBTITLE 1,Enriquecimento de Dados
from pyspark.sql import functions as F

# Tipagem e Derivações
# Esta seção aplica transformações estruturais básicas:
# - Tipagem correta (conversão de data_venda para date)
# - Derivação de campos temporais (ano, mes)
# - Metadata de auditoria (data_carga)

logger.log_info("Iniciando tipagem e derivações")

# Garantir que data_venda é date (PySpark)
df_clean = df_clean.withColumn("data_venda", F.to_date(F.col("data_venda")))

# Extrair ano e mês
df_clean = df_clean.withColumn("ano", F.year(F.col("data_venda")))
df_clean = df_clean.withColumn("mes", F.month(F.col("data_venda")))

# Adicionar timestamp de carga
df_clean = df_clean.withColumn("data_carga", F.current_timestamp())

logger.log_success(f"Tipagem e derivações concluídas: adicionadas colunas ano, mes, data_carga")
logger.log_info(f"Schema final:")
df_clean.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validações de Qualidade

# COMMAND ----------

# DBTITLE 1,Validações de Qualidade
from pyspark.sql import functions as F

logger.log_info("Executando validações de qualidade")

# Validação 1: Valores nulos
logger.log_info("Validação 1/5: Verificando valores nulos")
for col_name in df_clean.columns:
    null_count = df_clean.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        logger.log_warning(f"  - Coluna '{col_name}': {null_count} valores nulos")
        
logger.log_info("Validação 1/5: Concluída")

# Validação 2: Range de codigo_vendedor
invalid_codes_count = df_clean.filter(
    (F.col("codigo_vendedor") < 1) | (F.col("codigo_vendedor") > 8)
).count()

if invalid_codes_count > 0:
    logger.log_warning(f"Validação 2/5: {invalid_codes_count} códigos de vendedor inválidos (fora do range 1-8)")
else:
    logger.log_info("Validação 2/5: Todos os códigos de vendedor estão no range 1-8")

# Validação 3: Regiões válidas
valid_regions = ['Norte', 'Sul', 'Nordeste', 'Sudeste']
invalid_regions_count = df_clean.filter(
    ~F.col("regiao").isin(valid_regions)
).count()

if invalid_regions_count > 0:
    logger.log_warning(f"Validação 3/5: {invalid_regions_count} regiões inválidas encontradas")
else:
    logger.log_info("Validação 3/5: Todas as regiões são válidas")

# Validação 4: Valores de vendas positivos
negative_sales_count = df_clean.filter(F.col("valor_vendas") <= 0).count()

if negative_sales_count > 0:
    logger.log_warning(f"Validação 4/5: {negative_sales_count} vendas não-positivas encontradas")
else:
    logger.log_info("Validação 4/5: Todos os valores de vendas são positivos")

# Validação 5: Duplicatas
dup_count = df_clean.groupBy("data_venda", "vendedor", "secao") \
    .count() \
    .filter(F.col("count") > 1) \
    .count()

if dup_count > 0:
    logger.log_warning(f"Validação 5/5: {dup_count} grupos duplicados encontrados")
else:
    logger.log_info("Validação 5/5: Nenhuma duplicata encontrada")

logger.log_success("Validações de qualidade concluídas")

# COMMAND ----------

# MAGIC %md
# MAGIC    
# MAGIC ## 7. Criar Schema (se não existir)

# COMMAND ----------

# DBTITLE 1,Criar Schema
logger.log_info(f"Verificando/criando schema {TARGET_SCHEMA}")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

logger.log_success(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} verificado/criado")

# COMMAND ----------

# MAGIC %md
# MAGIC    
# MAGIC ## 8. Escrita na Tabela Delta

# COMMAND ----------

# DBTITLE 1,Escrita na Tabela Delta
try:
    logger.log_info(f"Iniciando escrita na tabela {FULL_TABLE_NAME} (modo: {WRITE_MODE})")
    
    # Escrever tabela Delta (df_clean já é PySpark desde a célula 8)
    df_clean.write \
        .format("delta") \
        .mode(WRITE_MODE) \
        .option("overwriteSchema", "true") \
        .saveAsTable(FULL_TABLE_NAME)
    
    logger.log_success(f"Tabela {FULL_TABLE_NAME} criada/atualizada com sucesso")
except Exception as e:
    logger.log_error(f"Erro ao escrever tabela {FULL_TABLE_NAME}")
    logger.error_handler(e, debug_write_mode=False)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC    
# MAGIC ## 9. Validações Pós-Carga

# COMMAND ----------

# DBTITLE 1,Validações Pós-Carga
logger.log_info("Executando validações pós-carga")

# Ler dados da tabela Delta para validação
df_delta = spark.table(FULL_TABLE_NAME)

record_count = df_delta.count()
logger.log_info(f"Contagem de registros na tabela: {record_count}")

logger.log_info("Schema da tabela Delta:")
df_delta.printSchema()

logger.log_info("Exibindo agregações de sanidade por região")
display(
    df_delta.groupBy("regiao")
    .agg(
        F.count("*").alias("qtd_vendas"),
        F.sum("valor_vendas").alias("total_vendas"),
        F.avg("valor_vendas").alias("ticket_medio")
    )
)

logger.log_success("Validações pós-carga concluídas")

# COMMAND ----------

# MAGIC %md
# MAGIC    
# MAGIC ## 10. Métricas Finais

# COMMAND ----------

# DBTITLE 1,Validação de Carga Métricas Finais
from datetime import datetime

logger.log_info("="*80)
logger.log_info("RESUMO DA INGESTÃO")
logger.log_info("="*80)
logger.log_info(f"Fonte: Pasta data/ (path relativo: {DATA_DIR})")
logger.log_info(f"Tabela destino: {FULL_TABLE_NAME}")
logger.log_info(f"Modo de escrita: {WRITE_MODE}")
logger.log_info(f"Registros na tabela: {record_count}")
logger.log_info(f"Processamento: PySpark distribuído (conversão imediata de pandas)")
logger.log_info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
logger.log_info("="*80)
logger.log_success("=== INGESTÃO CONCLUÍDA COM SUCESSO ===")
