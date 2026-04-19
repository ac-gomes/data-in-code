# Especificação Técnica: Vendas Base Ingestion

## Visão Geral

Feature responsável por ler arquivos CSV sintéticos da pasta `data/` e persistir em uma tabela Delta otimizada para análise com PySpark no Unity Catalog.

**Versão**: 3.1.0  
**Notebook**: `ingest_vendas_base`  
**Path**: `/Users/data.in.code/data-in-code/vendas_regionais/src/ingest_vendas_base`

## Arquitetura de Dados

### Input: Arquivos CSV

* **Path**: `/Workspace/Users/data.in.code/data-in-code/vendas_regionais/data/`
* **Pattern**: `*.csv` (todos os arquivos CSV da pasta)
* **Engine**: pandas (leitura inicial) + spark.createDataFrame (conversão imediata)
* **Volume**: ~1000 registros (dados sintéticos)

#### Schema de Input (CSV)

| Coluna | Tipo Pandas | Tipo PySpark | Descrição | Validação |
|--------|-------------|--------------|-----------|-----------|
| Data da Venda | object | StringType → DateType | Data da transação | Not null, formato válido |
| Mês | object | StringType | Mês abreviado (PT-BR) | Not null |
| Região | object | StringType | Região geográfica | Not null, IN ('Norte', 'Sul', 'Sudeste', 'Nordeste') |
| Vendedor | object | StringType | Nome do vendedor | Not null |
| Código Vendedor | int64 | LongType | ID único do vendedor | Not null, 1-8 |
| Seção | object | StringType | Seção/categoria do produto | Not null |
| Vendas | float64 | DoubleType | Valor da venda | Not null, > 0 |

### Output: Tabela Delta (Unity Catalog)

* **Catalog**: `workspace` (Unity Catalog)
* **Schema**: `vendas_regionais` (criado automaticamente se não existir)
* **Table Name**: `vendas_base`
* **Full Qualified Name**: `workspace.vendas_regionais.vendas_base`
* **Format**: Delta Lake
* **Mode**: Overwrite (carga completa)

#### Schema de Output (Delta)

```sql
CREATE TABLE IF NOT EXISTS workspace.vendas_regionais.vendas_base (
  data_venda DATE NOT NULL,
  mes_abrev STRING NOT NULL,
  regiao STRING NOT NULL,
  vendedor STRING NOT NULL,
  codigo_vendedor LONG NOT NULL,
  secao STRING NOT NULL,
  valor_vendas DOUBLE NOT NULL,
  ano INT NOT NULL,
  mes INT NOT NULL,
  data_carga TIMESTAMP NOT NULL
) 
USING DELTA;
```

**Observações**:
* Nomes de colunas em snake_case (padrão Python/SQL)
* Colunas derivadas: `ano` (year), `mes` (month number), `data_carga` (ingest timestamp)
* Tipo DoubleType para valores monetários (precisão financeira)
* Total de 10 colunas (7 originais + 3 derivadas)

## Fluxo de Processamento (v3.1.0)

### 1. Inicialização e Configuração

```python
# Importar LogControl
%run ../../error_handler_logging/src/logger_control

# Configurar logger
logger = LogControl(
    logger_name="vendas_base_ingestion",
    tbl_name="main.vendas_regionais.tb_logs_ingestion"
)

# Parâmetros
DATA_DIR = "../data"
TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "vendas_regionais"
TARGET_TABLE = "vendas_base"
FULL_TABLE_NAME = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"
WRITE_MODE = "overwrite"
```

### 2. Leitura dos CSV com Conversão Imediata

```python
import os
import pandas as pd
from pyspark.sql import functions as F

try:
    logger.log_info("Iniciando leitura dos arquivos CSV")
    
    # Path absoluto
    workspace_base = "/Workspace/Users/data.in.code/data-in-code/vendas_regionais"
    data_dir_abs = f"{workspace_base}/data"
    
    # Listar arquivos CSV
    arquivos = os.listdir(data_dir_abs)
    arquivos_csv = [f for f in arquivos if f.endswith('.csv')]
    
    # Ler e converter IMEDIATAMENTE para Spark
    dfs_spark = []
    for arquivo in arquivos_csv:
        csv_path = os.path.join(data_dir_abs, arquivo)
        df_temp_pandas = pd.read_csv(csv_path)
        df_temp_spark = spark.createDataFrame(df_temp_pandas)  # Conversão imediata
        dfs_spark.append(df_temp_spark)
        logger.log_info(f"✓ Lido e convertido: {arquivo}")
    
    # Union de todos os DataFrames Spark
    df_raw = dfs_spark[0]
    for df in dfs_spark[1:]:
        df_raw = df_raw.union(df)
    
    logger.log_success(f"Leitura concluída: {df_raw.count()} registros")
    
except Exception as e:
    logger.error_handler(e)
    raise
```

**Características**:
* Conversão imediata pandas → PySpark (evita overhead de memória)
* Suporte a múltiplos arquivos CSV via union
* Processamento distribuído desde o início

### 3. Limpeza de Dados (PySpark)

```python
logger.log_info("Iniciando limpeza de dados")

# Remover colunas Unnamed
cols_to_keep = [col for col in df_raw.columns if not col.startswith('Unnamed')]
df_clean = df_raw.select(cols_to_keep)

# Remover linhas vazias
filter_condition = None
for col in cols_to_keep:
    if filter_condition is None:
        filter_condition = F.col(col).isNotNull()
    else:
        filter_condition = filter_condition | F.col(col).isNotNull()

df_clean = df_clean.filter(filter_condition)

# Renomear para snake_case
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

# Trim de strings
for col_name in df_clean.columns:
    col_type = dict(df_clean.dtypes)[col_name]
    if col_type == 'string':
        df_clean = df_clean.withColumn(col_name, F.trim(F.col(col_name)))

logger.log_success("Limpeza concluída")
```

### 4. Tipagem e Derivações (PySpark)

```python
logger.log_info("Iniciando tipagem e derivações")

# Converter data_venda para DateType
df_clean = df_clean.withColumn("data_venda", F.to_date(F.col("data_venda")))

# Derivar ano e mês
df_clean = df_clean.withColumn("ano", F.year(F.col("data_venda")))
df_clean = df_clean.withColumn("mes", F.month(F.col("data_venda")))

# Adicionar timestamp de carga
df_clean = df_clean.withColumn("data_carga", F.current_timestamp())

logger.log_success("Tipagem concluída")
df_clean.printSchema()
```

### 5. Validações de Qualidade (PySpark)

```python
logger.log_info("Executando validações de qualidade")

# Validação 1: Nulos
for col_name in df_clean.columns:
    null_count = df_clean.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        logger.log_warning(f"Coluna '{col_name}': {null_count} nulos")

# Validação 2: Range de codigo_vendedor
invalid_codes = df_clean.filter(
    (F.col("codigo_vendedor") < 1) | (F.col("codigo_vendedor") > 8)
).count()

if invalid_codes > 0:
    logger.log_warning(f"{invalid_codes} códigos de vendedor inválidos")

# Validação 3: Regiões válidas
valid_regions = ['Norte', 'Sul', 'Nordeste', 'Sudeste']
invalid_regions = df_clean.filter(~F.col("regiao").isin(valid_regions)).count()

if invalid_regions > 0:
    logger.log_warning(f"{invalid_regions} regiões inválidas")

# Validação 4: Valores positivos
negative_sales = df_clean.filter(F.col("valor_vendas") <= 0).count()

if negative_sales > 0:
    logger.log_warning(f"{negative_sales} vendas não-positivas")

# Validação 5: Duplicatas
dup_count = df_clean.groupBy("data_venda", "vendedor", "secao") \
    .count() \
    .filter(F.col("count") > 1) \
    .count()

if dup_count > 0:
    logger.log_warning(f"{dup_count} grupos duplicados")

logger.log_success("Validações concluídas")
```

### 6. Criar Schema e Escrever Delta

```python
try:
    logger.log_info(f"Verificando/criando schema {TARGET_SCHEMA}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
    logger.log_success(f"Schema {TARGET_CATALOG}.{TARGET_SCHEMA} verificado")
    
    logger.log_info(f"Iniciando escrita na tabela {FULL_TABLE_NAME}")
    
    df_clean.write \
        .format("delta") \
        .mode(WRITE_MODE) \
        .option("overwriteSchema", "true") \
        .saveAsTable(FULL_TABLE_NAME)
    
    logger.log_success(f"Tabela {FULL_TABLE_NAME} criada/atualizada")
    
except Exception as e:
    logger.error_handler(e)
    raise
```

### 7. Validações Pós-Carga (Lendo da Tabela)

```python
logger.log_info("Executando validações pós-carga")

# Ler da tabela Delta (fonte de verdade)
df_delta = spark.table(FULL_TABLE_NAME)

record_count = df_delta.count()
logger.log_info(f"Registros na tabela: {record_count}")

logger.log_info("Schema da tabela Delta:")
df_delta.printSchema()

# Exibir amostra
display(df_delta.limit(10))

# Agregações de sanidade
display(
    df_delta.groupBy("regiao")
    .agg(
        F.count("*").alias("qtd_vendas"),
        F.sum("valor_vendas").alias("total_vendas"),
        F.avg("valor_vendas").alias("ticket_medio")
    )
)

logger.log_success("Validações pós-carga concluídas")
```

### 8. Métricas Finais

```python
from datetime import datetime

logger.log_info("="*80)
logger.log_info("RESUMO DA INGESTÃO")
logger.log_info("="*80)
logger.log_info(f"Fonte: Pasta data/ (path relativo: {DATA_DIR})")
logger.log_info(f"Tabela destino: {FULL_TABLE_NAME}")
logger.log_info(f"Modo de escrita: {WRITE_MODE}")
logger.log_info(f"Registros na tabela: {record_count}")
logger.log_info(f"Processamento: PySpark distribuído (conversão imediata)")
logger.log_info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
logger.log_info("="*80)
logger.log_success("=== INGESTÃO CONCLUÍDA COM SUCESSO ===")
```

## Tratamento de Erros

### Exceções Esperadas

1. **FileNotFoundError**: Pasta data/ não encontrada ou sem arquivos CSV
   * Ação: Logar erro com stack trace, interromper execução
   
2. **AnalysisException**: Erro na criação/escrita da tabela Delta (Unity Catalog)
   * Ação: Logar erro com stack trace, interromper execução
   
3. **Py4JJavaError**: Erro no processamento PySpark
   * Ação: Logar erro com stack trace, interromper execução

### Padrão de Captura

```python
try:
    # Código de ingestão
    pass
except Exception as e:
    logger.error_handler(e)
    raise  # Re-lançar para interromper execução
```

## Métricas e Monitoramento

### Logs Obrigatórios

* Início do processo de ingestão
* Contagem de arquivos CSV lidos
* Contagem de registros lidos
* Contagem de registros escritos na Delta
* Tempo de execução total
* Quaisquer warnings de qualidade de dados

### Validações Pós-Carga

* Contagem de registros na tabela Delta (lendo da tabela)
* Schema da tabela (verificação de tipos)
* Agregações por região (sanidade)
* Amostra de 10 registros

## Performance

* **Volume**: ~1000 registros (baixo volume)
* **Tempo Esperado**: < 1 minuto
* **Particionamento**: Não necessário devido ao baixo volume
* **Cache**: Não necessário
* **Compute**: Databricks Serverless (escala automaticamente)

## Dependências

* PySpark (Databricks Runtime)
* Pandas (leitura inicial dos CSV)
* Unity Catalog (workspace catalog ativo)
* LogControl (feature error_handler_logging)
* Databricks Serverless ou cluster com Unity Catalog habilitado

## Diferenças da Versão Anterior

### v3.1.0 vs v3.0.0

* ✅ **Removido**: Célula redundante de "Conversão para Spark DataFrame"
* ✅ **Removido**: Exposição de DataFrame `spark_df` para outros notebooks
* ✅ **Adicionado**: Validações pós-carga lendo DA TABELA (evita duplicação)
* ✅ **Simplificado**: Fluxo direto CSV → PySpark → Delta → Validações
* ✅ **Padronizado**: Unity Catalog (workspace.vendas_regionais.vendas_base)
