# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# DBTITLE 1,Vendas Semantic Layer - Header
# MAGIC %md
# MAGIC    
# MAGIC # Vendas Semantic Layer - Criação de Views
# MAGIC
# MAGIC Cria 4 temp views SQL que agregam dados da **tabela Delta** `workspace.vendas_regionais.vendas_base`.
# MAGIC
# MAGIC **Feature**: vendas_semantic_layer  
# MAGIC **Versão**: 2.0.0 (atualizada para v3.1.0 do ingest_vendas_base)
# MAGIC **Última Atualização**: 2026-04-19
# MAGIC
# MAGIC ## Arquitetura do Pipeline
# MAGIC ```
# MAGIC nb_synthetic_data_generator 
# MAGIC   ↓ salva em data/
# MAGIC nb_vendas_base_ingestion
# MAGIC   ↓ persiste em Delta (workspace.vendas_regionais.vendas_base)
# MAGIC nb_create_semantic_views
# MAGIC   ↓ lê da tabela Delta e cria temp views SQL
# MAGIC Views disponíveis para análise
# MAGIC ```
# MAGIC
# MAGIC ## Temp Views Criadas
# MAGIC 1. `vw_vendas_por_vendedor` - Vendas totais por vendedor
# MAGIC 2. `vw_vendas_por_regiao` - Vendas totais por região
# MAGIC 3. `vw_vendas_por_mes` - Vendas totais por mês
# MAGIC 4. `vw_vendas_por_secao` - Vendas totais por seção/categoria
# MAGIC
# MAGIC ## Fonte de Dados
# MAGIC * **Tabela Delta** no Unity Catalog: `workspace.vendas_regionais.vendas_base`
# MAGIC * Dados sintéticos gerados por `nb_synthetic_data_generator`
# MAGIC * Processamento híbrido: Delta (persistido) + Temp Views (sessão)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup e Configuração

# COMMAND ----------

# DBTITLE 1,Setup - LogControl
# MAGIC %run ./logger_control

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Configurar Logger
# Configurar logger
logger = LogControl(
    logger_name="vendas_semantic_layer",
    tbl_name="main.vendas_regionais.tb_logs_semantic"
)

logger.log_info("="*80)
logger.log_info("=== INICIANDO CRIAÇÃO DAS VIEWS SEMÂNTICAS ===")
logger.log_info("Pipeline: Synthetic Data Generator → Base Ingestion → Semantic Views")
logger.log_info("="*80)

# COMMAND ----------

# DBTITLE 1,Criar Temp View com DataFrame
# Criar temp view lendo DIRETAMENTE da tabela Delta
logger.log_info("Lendo tabela Delta: workspace.vendas_regionais.vendas_base")

try:
    # Configurar tabela de origem (Unity Catalog)
    TABLE_NAME = "workspace.vendas_regionais.vendas_base"
    
    # Ler tabela Delta (fonte de verdade)
    df = spark.table(TABLE_NAME)
    
    # Validar que a tabela existe e tem dados
    record_count = df.count()
    
    if record_count == 0:
        raise ValueError(f"Tabela {TABLE_NAME} está vazia. Execute o nb_vendas_base_ingestion primeiro.")
    
    # Criar temp view para uso nas queries SQL
    df.createOrReplaceTempView("vendas_base_temp")
    
    logger.log_success(f"Temp view 'vendas_base_temp' criada com {record_count} registros")
    logger.log_info(f"Fonte: Tabela Delta {TABLE_NAME}")
    logger.log_info(f"Colunas: {df.columns}")
    
    # Exibir amostra dos dados
    print("\n" + "="*80)
    print("✅ Temp View 'vendas_base_temp' criada com sucesso!")
    print(f"Fonte: Tabela Delta {TABLE_NAME}")
    print(f"Registros: {record_count}")
    print(f"Colunas: {df.columns}")
    print("="*80)
    
    # Exibir amostra de 5 registros
    logger.log_info("Amostra dos dados (5 primeiros registros):")
    display(df.limit(5))
    
except Exception as e:
    logger.log_error("Erro ao criar temp view da tabela Delta")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validar Tabela Base

# COMMAND ----------

# DBTITLE 1,Validar Tabela Base
# Validar que a tabela Delta existe e está acessível
logger.log_info("Validando tabela base workspace.vendas_regionais.vendas_base")

try:
    # Verificar schema
    logger.log_info("Schema da tabela:")
    df.printSchema()
    
    # Estatísticas básicas
    logger.log_info("Estatísticas da tabela:")
    
    total_registros = df.count()
    total_vendas = df.select(spark_sum("valor_vendas")).collect()[0][0]
    
    logger.log_info(f"  Total de registros: {total_registros}")
    logger.log_info(f"  Total de vendas: R$ {total_vendas:,.2f}")
    
    # Verificar período de dados
    min_date = df.select("data_venda").agg({"data_venda": "min"}).collect()[0][0]
    max_date = df.select("data_venda").agg({"data_venda": "max"}).collect()[0][0]
    
    logger.log_info(f"  Período: {min_date} a {max_date}")
    
    logger.log_success("Tabela base validada com sucesso")
    
except Exception as e:
    logger.log_error("Erro ao validar tabela base")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Views

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 View: vw_vendas_por_vendedor

# COMMAND ----------

# DBTITLE 1,View: vw_vendas_por_vendedor
try:
    logger.log_info("Criando view: vw_vendas_por_vendedor")
    
    sql_vendedor = """
    CREATE OR REPLACE TEMP VIEW vw_vendas_por_vendedor AS
    SELECT 
        vendedor,
        SUM(valor_vendas) AS total_vendas,
        COUNT(*) AS qtd_transacoes,
        ROUND(AVG(valor_vendas), 2) AS ticket_medio
    FROM vendas_base_temp
    GROUP BY vendedor
    ORDER BY total_vendas DESC
    """
    
    spark.sql(sql_vendedor)
    
    # Validar criação
    df_vendedor = spark.sql("SELECT * FROM vw_vendas_por_vendedor")
    count_vendedor = df_vendedor.count()
    
    logger.log_success(f"✅ View vw_vendas_por_vendedor criada com {count_vendedor} vendedores")
    
except Exception as e:
    logger.log_error("Erro ao criar vw_vendas_por_vendedor")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 View: vw_vendas_por_regiao

# COMMAND ----------

# DBTITLE 1,View: vw_vendas_por_regiao
try:
    logger.log_info("Criando view: vw_vendas_por_regiao")
    
    sql_regiao = """
    CREATE OR REPLACE TEMP VIEW vw_vendas_por_regiao AS
    SELECT 
        regiao,
        SUM(valor_vendas) AS total_vendas,
        COUNT(*) AS qtd_transacoes,
        COUNT(DISTINCT vendedor) AS qtd_vendedores
    FROM vendas_base_temp
    GROUP BY regiao
    ORDER BY total_vendas DESC
    """
    
    spark.sql(sql_regiao)
    
    # Validar criação
    df_regiao = spark.sql("SELECT * FROM vw_vendas_por_regiao")
    count_regiao = df_regiao.count()
    
    logger.log_success(f"✅ View vw_vendas_por_regiao criada com {count_regiao} regiões")
    
except Exception as e:
    logger.log_error("Erro ao criar vw_vendas_por_regiao")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 View: vw_vendas_por_mes

# COMMAND ----------

# DBTITLE 1,View: vw_vendas_por_mes
try:
    logger.log_info("Criando view: vw_vendas_por_mes")
    
    sql_mes = """
    CREATE OR REPLACE TEMP VIEW vw_vendas_por_mes AS
    SELECT 
        mes_abrev as mes,
        SUM(valor_vendas) AS total_vendas,
        COUNT(*) AS qtd_transacoes,
        COUNT(DISTINCT vendedor) AS qtd_vendedores_ativos
    FROM vendas_base_temp
    GROUP BY mes_abrev
    ORDER BY 
        CASE mes_abrev
            WHEN 'JAN' THEN 1
            WHEN 'FEV' THEN 2
            WHEN 'MAR' THEN 3
            WHEN 'ABR' THEN 4
            WHEN 'MAI' THEN 5
            WHEN 'JUN' THEN 6
            WHEN 'JUL' THEN 7
            WHEN 'AGO' THEN 8
            WHEN 'SET' THEN 9
            WHEN 'OUT' THEN 10
            WHEN 'NOV' THEN 11
            WHEN 'DEZ' THEN 12
            ELSE 99
        END
    """
    
    spark.sql(sql_mes)
    
    # Validar criação
    df_mes = spark.sql("SELECT * FROM vw_vendas_por_mes")
    count_mes = df_mes.count()
    
    logger.log_success(f"✅ View vw_vendas_por_mes criada com {count_mes} meses")
    
except Exception as e:
    logger.log_error("Erro ao criar vw_vendas_por_mes")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 View: vw_vendas_por_secao

# COMMAND ----------

# DBTITLE 1,View: vw_vendas_por_secao
try:
    logger.log_info("Criando view: vw_vendas_por_secao")
    
    sql_secao = """
    CREATE OR REPLACE TEMP VIEW vw_vendas_por_secao AS
    SELECT 
        secao,
        SUM(valor_vendas) AS total_vendas,
        COUNT(*) AS qtd_transacoes,
        ROUND(AVG(valor_vendas), 2) AS ticket_medio
    FROM vendas_base_temp
    GROUP BY secao
    ORDER BY total_vendas DESC
    """
    
    spark.sql(sql_secao)
    
    # Validar criação
    df_secao = spark.sql("SELECT * FROM vw_vendas_por_secao")
    count_secao = df_secao.count()
    
    logger.log_success(f"✅ View vw_vendas_por_secao criada com {count_secao} seções")
    
except Exception as e:
    logger.log_error("Erro ao criar vw_vendas_por_secao")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validar Todas as Views

# COMMAND ----------

# DBTITLE 1,Validar Todas as Views
try:
    logger.log_info("Validando todas as temp views criadas")
    
    views = [
        "vw_vendas_por_vendedor",
        "vw_vendas_por_regiao",
        "vw_vendas_por_mes",
        "vw_vendas_por_secao"
    ]
    
    for view_name in views:
        df_view = spark.sql(f"SELECT * FROM {view_name}")
        count = df_view.count()
        total = df_view.select(spark_sum("total_vendas")).collect()[0][0]
        logger.log_info(f"  {view_name}: {count} registros, R$ {total:,.2f}")
    
    logger.log_success("✅ Todas as 4 temp views validadas com sucesso")
    
except Exception as e:
    logger.log_error("Erro durante validação das views")
    logger.error_handler(e, debug_write_mode=True)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exibir Resultados

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Vendas por Vendedor

# COMMAND ----------

# DBTITLE 1,Exibir: Vendas por Vendedor
display(spark.sql("SELECT * FROM vw_vendas_por_vendedor"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Vendas por Região

# COMMAND ----------

# DBTITLE 1,Exibir: Vendas por Região
display(spark.sql("SELECT * FROM vw_vendas_por_regiao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Vendas por Mês

# COMMAND ----------

# DBTITLE 1,Exibir: Vendas por Mês
display(spark.sql("SELECT * FROM vw_vendas_por_mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Vendas por Seção

# COMMAND ----------

# DBTITLE 1,Exibir: Vendas por Seção
display(spark.sql("SELECT * FROM vw_vendas_por_secao"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Sumário Final

# COMMAND ----------

# DBTITLE 1,Sumário Final
logger.log_success("=== CRIAÇÃO DAS VIEWS SEMÂNTICAS CONCLUÍDA COM SUCESSO ===")
logger.log_info("Temp Views criadas (disponíveis nesta sessão):")
logger.log_info("  1. vw_vendas_por_vendedor")
logger.log_info("  2. vw_vendas_por_regiao")
logger.log_info("  3. vw_vendas_por_mes")
logger.log_info("  4. vw_vendas_por_secao")

print("\n✅ Camada semântica criada com sucesso!")
print("\n📋 Temp Views disponíveis nesta sessão:")
print("  - vw_vendas_por_vendedor")
print("  - vw_vendas_por_regiao")
print("  - vw_vendas_por_mes")
print("  - vw_vendas_por_secao")
print("\n📦 Fonte de dados: Tabela Delta workspace.vendas_regionais.vendas_base")
print("="*80)
