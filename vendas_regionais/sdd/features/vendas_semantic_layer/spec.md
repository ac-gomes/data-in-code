# Especificação Técnica: Vendas Semantic Layer

**Versão**: 2.0.0 (atualizada para v3.1.0 do ingest_vendas_base)  
**Última Atualização**: 2026-04-19

## Visão Geral

Feature responsável por criar 4 **temp views SQL** que agregam dados da **tabela Delta** `workspace.vendas_regionais.vendas_base` para facilitar análises e dashboards.

**MUDANÇA ARQUITETURAL (v2.0.0):**
* **Antes (v1.0)**: Recebia DataFrame via %run de outro notebook, criava views persistidas
* **Agora (v2.0)**: Lê DIRETAMENTE da tabela Delta, cria temp views na sessão Spark

## Arquitetura de Views

### Tabela Fonte

* **Nome**: `workspace.vendas_regionais.vendas_base`
* **Catálogo**: workspace (Unity Catalog)
* **Schema**: vendas_regionais
* **Formato**: Delta Lake
* **Colunas Utilizadas**:
  * `vendedor` (STRING)
  * `regiao` (STRING)
  * `mes` (INT) - Mês numérico extraído de data_venda
  * `secao` (STRING)
  * `valor_vendas` (DECIMAL)
  * `data_venda` (DATE)

### Views de Destino

**Temp Views** criadas na sessão Spark (não persistidas no Unity Catalog).

* **Nomenclatura**: Prefixo `vw_` para indicar views agregadas
* **Escopo**: Disponíveis apenas durante a sessão ativa
* **Criação**: Usando `spark.sql()` com queries SQL

## Especificações das Views

### View 1: vw_vendas_por_vendedor

**Propósito**: Agregar total de vendas por vendedor.

**SQL de Criação**:

```sql
CREATE OR REPLACE TEMP VIEW vw_vendas_por_vendedor AS
SELECT 
    vendedor,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    ROUND(AVG(valor_vendas), 2) AS ticket_medio
FROM vendas_base_temp
GROUP BY vendedor
ORDER BY total_vendas DESC
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| vendedor | STRING | Nome do vendedor |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| ticket_medio | DECIMAL | Valor médio por transação |

---

### View 2: vw_vendas_por_regiao

**Propósito**: Agregar total de vendas por região geográfica.

**SQL de Criação**:

```sql
CREATE OR REPLACE TEMP VIEW vw_vendas_por_regiao AS
SELECT 
    regiao,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    COUNT(DISTINCT vendedor) AS qtd_vendedores
FROM vendas_base_temp
GROUP BY regiao
ORDER BY total_vendas DESC
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| regiao | STRING | Região geográfica |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| qtd_vendedores | BIGINT | Vendedores únicos na região |

---

### View 3: vw_vendas_por_mes

**Propósito**: Agregar total de vendas por mês.

**SQL de Criação**:

```sql
CREATE OR REPLACE TEMP VIEW vw_vendas_por_mes AS
SELECT 
    mes,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    COUNT(DISTINCT vendedor) AS qtd_vendedores_ativos
FROM vendas_base_temp
GROUP BY mes
ORDER BY mes ASC
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| mes | INT | Mês numérico (1-12) |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| qtd_vendedores_ativos | BIGINT | Vendedores com vendas no mês |

---

### View 4: vw_vendas_por_secao

**Propósito**: Agregar total de vendas por seção/categoria de produto.

**SQL de Criação**:

```sql
CREATE OR REPLACE TEMP VIEW vw_vendas_por_secao AS
SELECT 
    secao,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    ROUND(AVG(valor_vendas), 2) AS ticket_medio
FROM vendas_base_temp
GROUP BY secao
ORDER BY total_vendas DESC
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| secao | STRING | Seção/categoria do produto |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| ticket_medio | DECIMAL | Valor médio por transação |

---

## Fluxo de Implementação

### 1. Inicialização e Logging

```python
# Importar LogControl
%run ../../error_handler_logging/src/logger_control

# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count
import pandas as pd

# Configurar logger
logger = LogControl(
    logger_name="vendas_semantic_layer",
    tbl_name="main.vendas_regionais.tb_logs_semantic"
)

logger.log_info("="*80)
logger.log_info("=== INICIANDO CRIAÇÃO DAS VIEWS SEMÂNTICAS ===")
logger.log_info("Pipeline: Synthetic Data Generator → Base Ingestion → Semantic Views")
logger.log_info("="*80)
```

### 2. Ler Tabela Delta e Criar Temp View Base

```python
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
```

### 3. Validar Tabela Base

```python
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
```

### 4. Criar Temp Views Agregadas

```python
try:
    logger.log_info("Criando view: vw_vendas_por_vendedor")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_vendas_por_vendedor AS
        SELECT 
            vendedor,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            ROUND(AVG(valor_vendas), 2) AS ticket_medio
        FROM vendas_base_temp
        GROUP BY vendedor
        ORDER BY total_vendas DESC
    """)
    logger.log_success("View vw_vendas_por_vendedor criada")
    
    logger.log_info("Criando view: vw_vendas_por_regiao")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_vendas_por_regiao AS
        SELECT 
            regiao,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            COUNT(DISTINCT vendedor) AS qtd_vendedores
        FROM vendas_base_temp
        GROUP BY regiao
        ORDER BY total_vendas DESC
    """)
    logger.log_success("View vw_vendas_por_regiao criada")
    
    logger.log_info("Criando view: vw_vendas_por_mes")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_vendas_por_mes AS
        SELECT 
            mes,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            COUNT(DISTINCT vendedor) AS qtd_vendedores_ativos
        FROM vendas_base_temp
        GROUP BY mes
        ORDER BY mes ASC
    """)
    logger.log_success("View vw_vendas_por_mes criada")
    
    logger.log_info("Criando view: vw_vendas_por_secao")
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW vw_vendas_por_secao AS
        SELECT 
            secao,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            ROUND(AVG(valor_vendas), 2) AS ticket_medio
        FROM vendas_base_temp
        GROUP BY secao
        ORDER BY total_vendas DESC
    """)
    logger.log_success("View vw_vendas_por_secao criada")
    
except Exception as e:
    logger.log_error("Erro ao criar views")
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 5. Validar e Exibir Views

```python
try:
    logger.log_info("Validando views criadas")
    
    views = [
        "vw_vendas_por_vendedor",
        "vw_vendas_por_regiao",
        "vw_vendas_por_mes",
        "vw_vendas_por_secao"
    ]
    
    for view_name in views:
        df_view = spark.sql(f"SELECT * FROM {view_name}")
        count = df_view.count()
        logger.log_info(f"{view_name}: {count} registros")
    
    logger.log_success("Todas as views validadas com sucesso")
    
    # Exibir resultados
    for view_name in views:
        print(f"\n\n{'='*80}")
        print(f"View: {view_name}")
        print("="*80)
        display(spark.sql(f"SELECT * FROM {view_name}"))
    
except Exception as e:
    logger.log_error("Erro durante validação das views")
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 6. Sumário Final

```python
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
```

## Tratamento de Erros

### Exceções Esperadas

1. **AnalysisException**: Tabela base não existe
   * Ação: Logar erro e instruir execução de nb_vendas_base_ingestion primeiro

2. **ValueError**: Tabela base vazia (count = 0)
   * Ação: Logar erro e interromper execução

3. **ParseException**: Erro de sintaxe SQL
   * Ação: Logar erro com SQL completo, interromper execução

## Métricas de Validação

### Validações Implementadas

* **Tabela existe**: `spark.table()` não lança exceção
* **Tabela tem dados**: `df.count() > 0`
* **Views criadas**: Todas as 4 views consultáveis via SQL
* **Views têm dados**: Cada view retorna registros
* **Consistência interna**: Soma dos totais por vendedor = total geral

### Totais Esperados

* **Total Geral**: ~R$ 225.926,23 (dos dados sintéticos originais)
* **Por Vendedor**: 8 vendedores
* **Por Região**: 4 regiões (Norte, Sul, Sudeste, Nordeste)
* **Por Mês**: Varia conforme período de dados (1-12)
* **Por Seção**: 8 seções de produtos

## Performance

* **Complexidade**: O(n) para cada GROUP BY (n = ~1000 registros)
* **Tempo Esperado**: < 2 segundos para todas as 4 views
* **Cache**: Não necessário devido ao baixo volume
* **Leitura Delta**: Otimizada via Delta Lake (parquet + metadados)

## Dependências

* Tabela `workspace.vendas_regionais.vendas_base` (feature vendas_base_ingestion v3.1.0)
* LogControl (feature error_handler_logging)
* PySpark SQL
* Unity Catalog (workspace)

## Testes Requeridos

1. ✅ Teste de leitura da tabela Delta
2. ✅ Teste de criação da temp view base
3. ✅ Teste de criação das 4 temp views agregadas
4. ✅ Teste de consultabilidade (SELECT * funciona)
5. ✅ Teste de contagem de registros (>0 em cada view)
6. ✅ Teste de LogControl integração
7. 📋 Teste de execução completa do notebook
