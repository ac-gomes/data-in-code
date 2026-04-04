# Especificação Técnica: Vendas Base Ingestion

## Visão Geral

Feature responsável por ler dados da aba "Base" do arquivo Excel `VendasRegionaisVBA.xlsm` e persistir em uma tabela Delta otimizada para análise com PySpark.

## Arquitetura de Dados

### Input: Arquivo Excel

* **Path**: `/Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/arquivos/VendasRegionaisVBA.xlsm`
* **Sheet**: `Base`
* **Engine**: openpyxl (suporte a .xlsm com macros)

#### Schema de Input (Excel)

| Coluna | Tipo Pandas | Tipo PySpark | Descrição | Validação |
|--------|-------------|--------------|-----------|-----------|
| Data da Venda | datetime64[ns] | DateType | Data da transação | Not null, >= 2018-01-01 |
| Região | object | StringType | Região geográfica | Not null, IN ('Norte', 'Sul', 'Sudeste', 'Nordeste') |
| Vendedor | object | StringType | Nome do vendedor | Not null |
| Código Vendedor | int64 | IntegerType | ID único do vendedor | Not null, > 0 |
| Seção | object | StringType | Seção/categoria do produto | Not null |
| Vendas | float64 | DecimalType(10,2) | Valor da venda | Not null, > 0 |
| Mês | object | StringType | Mês abreviado (PT-BR) | Not null, IN ('JAN', 'FEV', 'MAR', 'ABR', 'MAI') |

**Nota**: Colunas "Unnamed" encontradas no Excel devem ser descartadas durante o processo de limpeza.

### Output: Tabela Delta

* **Catalog**: `main` (ou catalog padrão do workspace)
* **Schema**: `vendas_regionais` (criar se não existir)
* **Table Name**: `tb_vendas_base`
* **Full Qualified Name**: `main.vendas_regionais.tb_vendas_base`
* **Format**: Delta Lake
* **Mode**: Overwrite (carga completa)

#### Schema de Output (Delta)

```sql
CREATE TABLE IF NOT EXISTS main.vendas_regionais.tb_vendas_base (
  data_venda DATE NOT NULL,
  regiao STRING NOT NULL,
  vendedor STRING NOT NULL,
  codigo_vendedor INT NOT NULL,
  secao STRING NOT NULL,
  valor_vendas DECIMAL(10,2) NOT NULL,
  mes STRING NOT NULL,
  dt_carga TIMESTAMP NOT NULL
) 
USING DELTA;
```

**Observações**:
* Nomes de colunas em snake_case (padrão Python/SQL)
* Adição de coluna `dt_carga` (timestamp da ingestão)
* Tipo DecimalType para valores monetários (precisão financeira)

## Fluxo de Processamento

### 1. Inicialização

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
import pandas as pd

# Importar LogControl
%run "/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/sdd/features/error_handler_logging/src/logger_control"

# Instanciar logger
logger = LogControl(
    logger_name="vendas_base_ingestion",
    tbl_name="main.vendas_regionais.tb_logs_ingestion"
)
```

### 2. Leitura do Excel

```python
try:
    logger.log_info("Iniciando leitura do arquivo Excel")
    
    excel_path = "/Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/arquivos/VendasRegionaisVBA.xlsm"
    
    # Ler com pandas
    df_pandas = pd.read_excel(excel_path, sheet_name='Base')
    
    # Remover colunas Unnamed
    df_pandas_clean = df_pandas.loc[:, ~df_pandas.columns.str.contains('^Unnamed')]
    
    logger.log_success(f"Arquivo lido com sucesso: {len(df_pandas_clean)} registros")
    
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 3. Conversão para PySpark

```python
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
    
    logger.log_info(f"Conversão concluída. Schema: {df_spark.schema}")
    
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 4. Validação de Qualidade

```python
try:
    logger.log_info("Iniciando validações de qualidade")
    
    # Validação 1: Nulos
    null_counts = df_spark.select([sum(col(c).isNull().cast("int")).alias(c) 
                                     for c in df_spark.columns if c != "dt_carga"])
    
    # Validação 2: Valores de vendas positivos
    negative_sales = df_spark.filter(col("valor_vendas") <= 0).count()
    if negative_sales > 0:
        logger.log_warning(f"Encontrados {negative_sales} registros com vendas <= 0")
    
    # Validação 3: Regiões válidas
    valid_regions = ['Norte', 'Sul', 'Sudeste', 'Nordeste']
    invalid_regions = df_spark.filter(~col("regiao").isin(valid_regions)).count()
    if invalid_regions > 0:
        logger.log_warning(f"Encontradas {invalid_regions} regiões inválidas")
    
    logger.log_success("Validações de qualidade concluídas")
    
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 5. Persistência Delta

```python
try:
    logger.log_info("Iniciando escrita na tabela Delta")
    
    # Criar schema se não existir
    spark.sql("CREATE SCHEMA IF NOT EXISTS main.vendas_regionais")
    
    # Escrever tabela Delta
    df_spark.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("main.vendas_regionais.tb_vendas_base")
    
    # Validar contagem de registros
    count_written = spark.table("main.vendas_regionais.tb_vendas_base").count()
    
    logger.log_success(f"Tabela Delta criada com sucesso: {count_written} registros")
    
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)
    raise
```

## Tratamento de Erros

### Exceções Esperadas

1. **FileNotFoundError**: Arquivo Excel não encontrado
   * Ação: Logar erro com stack trace, interromper execução
   
2. **ValueError**: Sheet "Base" não existe no arquivo
   * Ação: Logar erro com stack trace, interromper execução
   
3. **SchemaException**: Schema do Excel diferente do esperado
   * Ação: Logar warning, tentar continuar se possível
   
4. **AnalysisException**: Erro na criação/escrita da tabela Delta
   * Ação: Logar erro com stack trace, interromper execução

### Padrão de Captura

```python
try:
    # Código de ingestão
    pass
except Exception as e:
    logger.error_handler(e, debug_write_mode=True)
    raise  # Re-lançar para interromper execução
```

## Métricas e Monitoramento

### Logs Obrigatórios

* Início do processo de ingestão
* Contagem de registros lidos do Excel
* Contagem de registros escritos na Delta
* Tempo de execução total
* Quaisquer warnings de qualidade de dados

### Validações Pós-Carga

```python
# Validação final
assert spark.table("main.vendas_regionais.tb_vendas_base").count() == expected_count
logger.log_success(f"Validação pós-carga: {expected_count} registros confirmados")
```

## Performance

* **Volume**: ~90 registros (baixo volume)
* **Tempo Esperado**: < 30 segundos
* **Particionamento**: Não necessário devido ao baixo volume
* **Cache**: Não necessário

## Dependências

* PySpark >= 3.0
* Pandas >= 1.0
* Openpyxl >= 3.0
* LogControl (feature error_handler_logging)

## Testes Requeridos

1. Teste de leitura do arquivo Excel
2. Teste de validação de schema
3. Teste de limpeza de colunas Unnamed
4. Teste de conversão de tipos
5. Teste de escrita Delta
6. Teste de validações de qualidade
7. Teste de integração com LogControl
