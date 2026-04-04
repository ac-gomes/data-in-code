# Databricks notebook source
# MAGIC %md
# MAGIC # Vendas Base Ingestion
# MAGIC 
# MAGIC Ingestão de dados da aba "Base" do arquivo VendasRegionaisVBA.xlsm para tabela Delta.
# MAGIC 
# MAGIC **Feature**: vendas_base_ingestion  
# MAGIC **Autor**: Sistema de Ingestão Automatizado  
# MAGIC **Última Atualização**: 2024

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuração

# COMMAND ----------

# Importar bibliotecas
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit, sum as spark_sum
from pyspark.sql.types import StringType, IntegerType, DecimalType, DateType, TimestampType
import pandas as pd

# Importar LogControl
%run "/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/sdd/features/error_handler_logging/src/logger_control"

# COMMAND ----------

# Configurar logger
logger = LogControl(
    logger_name="vendas_base_ingestion",
    tbl_name="main.vendas_regionais.tb_logs_ingestion"
)

logger.log_info("=== INICIANDO PROCESSO DE INGESTÃO ===")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura do Arquivo Excel

# COMMAND ----------

try:
    logger.log_info("Iniciando leitura do arquivo Excel")
    
    # Path do arquivo
    excel_path = "/Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/arquivos/VendasRegionaisVBA.xlsm"
    sheet_name = "Base"
    
    # Ler com pandas
    df_pandas = pd.read_excel(excel_path, sheet_name=sheet_name)
    
    logger.log_success(f"Arquivo lido com sucesso: {len(df_pandas)} registros, {len(df_pandas.columns)} colunas")
    
except FileNotFoundError as e:
    logger.log_error(f"Arquivo não encontrado: {excel_path}")
    logger.error_handler(e, debug_write_mode=True)
    raise
except ValueError as e:
    logger.log_error(f"Aba '{sheet_name}' não encontrada no arquivo")
    logger.error_handler(e, debug_write_mode=True)
    raise
except Exception as e:
    logger.log_error("Erro inesperado ao ler arquivo Excel")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza e Transformação de Dados

# COMMAND ----------

try:
    logger.log_info("Iniciando limpeza de dados")
    
    # Remover colunas Unnamed
    df_pandas_clean = df_pandas.loc[:, ~df_pandas.columns.str.contains('^Unnamed')]
    
    logger.log_info(f"Colunas após limpeza: {list(df_pandas_clean.columns)}")
    
    # Validar colunas esperadas
    expected_columns = ['Data da Venda', 'Região', 'Vendedor', 'Código Vendedor', 'Seção', 'Vendas', 'Mês']
    missing_columns = set(expected_columns) - set(df_pandas_clean.columns)
    
    if missing_columns:
        raise ValueError(f"Colunas faltando no arquivo: {missing_columns}")
    
    logger.log_success("Limpeza de dados concluída com sucesso")
    
except Exception as e:
    logger.log_error("Erro durante limpeza de dados")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

try:
    logger.log_info("Convertendo DataFrame Pandas para PySpark")
    
    # Criar Spark DataFrame
    df_spark = spark.createDataFrame(df_pandas_clean)
    
    # Renomear colunas para snake_case
    df_spark = df_spark \
        .withColumnRenamed("Data da Venda", "data_venda") \
        .withColumnRenamed("Região", "regiao") \
        .withColumnRenamed("Vendedor", "vendedor") \
        .withColumnRenamed("Código Vendedor", "codigo_vendedor") \
        .withColumnRenamed("Seção", "secao") \
        .withColumnRenamed("Vendas", "valor_vendas") \
        .withColumnRenamed("Mês", "mes")
    
    # Adicionar coluna de carga
    df_spark = df_spark.withColumn("dt_carga", current_timestamp())
    
    logger.log_info(f"Schema após transformações:\n{df_spark.schema.simpleString()}")
    logger.log_success("Conversão para PySpark concluída")
    
except Exception as e:
    logger.log_error("Erro durante conversão para PySpark")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validações de Qualidade

# COMMAND ----------

try:
    logger.log_info("Iniciando validações de qualidade")
    
    # Validação 1: Contagem de nulos
    null_counts = {}
    for column in ['data_venda', 'regiao', 'vendedor', 'codigo_vendedor', 'secao', 'valor_vendas', 'mes']:
        null_count = df_spark.filter(col(column).isNull()).count()
        null_counts[column] = null_count
        if null_count > 0:
            logger.log_warning(f"Coluna '{column}' tem {null_count} valores nulos")
    
    # Validação 2: Valores de vendas positivos
    negative_sales = df_spark.filter(col("valor_vendas") <= 0).count()
    if negative_sales > 0:
        logger.log_warning(f"Encontrados {negative_sales} registros com vendas <= 0")
    else:
        logger.log_info("Todos os valores de vendas são positivos")
    
    # Validação 3: Regiões válidas
    valid_regions = ['Norte', 'Sul', 'Sudeste', 'Nordeste']
    invalid_regions = df_spark.filter(~col("regiao").isin(valid_regions)).count()
    if invalid_regions > 0:
        logger.log_warning(f"Encontradas {invalid_regions} regiões inválidas")
        # Mostrar regiões inválidas
        invalid_region_list = df_spark.filter(~col("regiao").isin(valid_regions)).select("regiao").distinct().collect()
        logger.log_warning(f"Regiões inválidas: {[row.regiao for row in invalid_region_list]}")
    else:
        logger.log_info("Todas as regiões estão no conjunto válido")
    
    # Validação 4: Meses válidos
    valid_months = ['JAN', 'FEV', 'MAR', 'ABR', 'MAI']
    invalid_months = df_spark.filter(~col("mes").isin(valid_months)).count()
    if invalid_months > 0:
        logger.log_warning(f"Encontrados {invalid_months} meses inválidos")
        # Mostrar meses inválidos
        invalid_month_list = df_spark.filter(~col("mes").isin(valid_months)).select("mes").distinct().collect()
        logger.log_warning(f"Meses inválidos: {[row.mes for row in invalid_month_list]}")
    else:
        logger.log_info("Todos os meses estão no conjunto válido")
    
    # Sumário de validações
    total_records = df_spark.count()
    logger.log_info(f"Total de registros a serem carregados: {total_records}")
    logger.log_success("Validações de qualidade concluídas")
    
except Exception as e:
    logger.log_error("Erro durante validações de qualidade")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistência em Delta Table

# COMMAND ----------

try:
    logger.log_info("Criando schema se não existir")
    
    # Criar schema
    spark.sql("CREATE SCHEMA IF NOT EXISTS main.vendas_regionais")
    
    logger.log_success("Schema main.vendas_regionais disponível")
    
except Exception as e:
    logger.log_error("Erro ao criar schema")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

try:
    logger.log_info("Iniciando escrita na tabela Delta")
    
    table_name = "main.vendas_regionais.tb_vendas_base"
    
    # Escrever tabela Delta
    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    
    # Validar contagem de registros
    count_written = spark.table(table_name).count()
    
    logger.log_success(f"Tabela Delta '{table_name}' criada com sucesso: {count_written} registros")
    
    # Exibir amostra dos dados
    logger.log_info("Exibindo amostra dos dados carregados")
    display(spark.table(table_name).limit(10))
    
except Exception as e:
    logger.log_error(f"Erro ao escrever tabela Delta '{table_name}'")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validações Pós-Carga

# COMMAND ----------

try:
    logger.log_info("Executando validações pós-carga")
    
    table_name = "main.vendas_regionais.tb_vendas_base"
    
    # Validação 1: Tabela existe e é consultável
    df_loaded = spark.table(table_name)
    count_loaded = df_loaded.count()
    
    logger.log_info(f"Registros na tabela: {count_loaded}")
    
    # Validação 2: Comparar com contagem original
    if count_loaded == total_records:
        logger.log_success(f"Validação OK: {count_loaded} registros carregados conforme esperado")
    else:
        logger.log_warning(f"Divergência: Esperado {total_records}, carregado {count_loaded}")
    
    # Validação 3: Schema da tabela
    logger.log_info(f"Schema da tabela Delta:\n{df_loaded.schema.simpleString()}")
    
    # Validação 4: Estatísticas básicas
    stats = df_loaded.select(
        spark_sum("valor_vendas").alias("total_vendas"),
        col("data_venda").cast("date").alias("min_date"),
        col("data_venda").cast("date").alias("max_date")
    ).first()
    
    logger.log_info(f"Estatísticas: Total de vendas = R$ {stats.total_vendas:,.2f}")
    
    logger.log_success("Validações pós-carga concluídas")
    
except Exception as e:
    logger.log_error("Erro durante validações pós-carga")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Sumário Final

# COMMAND ----------

logger.log_success("=== PROCESSO DE INGESTÃO CONCLUÍDO COM SUCESSO ===")
logger.log_info(f"Tabela: main.vendas_regionais.tb_vendas_base")
logger.log_info(f"Registros: {count_loaded}")
logger.log_info(f"Total de Vendas: R$ {stats.total_vendas:,.2f}")
print("\n✅ Ingestão finalizada com sucesso!")
print(f"📊 Tabela: main.vendas_regionais.tb_vendas_base")
print(f"📈 Registros: {count_loaded}")
