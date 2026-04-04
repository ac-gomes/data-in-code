# Especificação Técnica: Vendas Semantic Layer

## Visão Geral

Feature responsável por criar 4 views SQL que agregam dados da tabela `main.vendas_regionais.tb_vendas_base` para facilitar análises e dashboards.

## Arquitetura de Views

### Tabela Fonte

* **Nome**: `main.vendas_regionais.tb_vendas_base`
* **Schema**: main.vendas_regionais
* **Formato**: Delta Lake
* **Colunas Utilizadas**:
  * `vendedor` (STRING)
  * `regiao` (STRING)
  * `mes` (STRING)
  * `secao` (STRING)
  * `valor_vendas` (DECIMAL)

### Views de Destino

Todas as views serão criadas no schema `main.vendas_regionais` com prefixo `vw_`.

## Especificações das Views

### View 1: vw_vendas_por_vendedor

**Propósito**: Agregar total de vendas por vendedor.

**SQL de Criação**:

```sql
CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_vendedor AS
SELECT 
    vendedor,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    AVG(valor_vendas) AS ticket_medio
FROM main.vendas_regionais.tb_vendas_base
GROUP BY vendedor
ORDER BY total_vendas DESC;
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| vendedor | STRING | Nome do vendedor |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| ticket_medio | DECIMAL | Valor médio por transação |

**Validação**: Comparar total_vendas com aba "Base Grafico" (colunas 0-1, linhas 3-10)

---

### View 2: vw_vendas_por_regiao

**Propósito**: Agregar total de vendas por região geográfica.

**SQL de Criação**:

```sql
CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_regiao AS
SELECT 
    regiao,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    COUNT(DISTINCT vendedor) AS qtd_vendedores
FROM main.vendas_regionais.tb_vendas_base
GROUP BY regiao
ORDER BY total_vendas DESC;
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| regiao | STRING | Região geográfica |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| qtd_vendedores | BIGINT | Vendedores únicos na região |

**Validação**: Comparar total_vendas com aba "Base Grafico" (colunas 3-4, linhas 13-16)

---

### View 3: vw_vendas_por_mes

**Propósito**: Agregar total de vendas por mês.

**SQL de Criação**:

```sql
CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_mes AS
SELECT 
    mes,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    COUNT(DISTINCT vendedor) AS qtd_vendedores_ativos
FROM main.vendas_regionais.tb_vendas_base
GROUP BY mes
ORDER BY 
    CASE mes
        WHEN 'JAN' THEN 1
        WHEN 'FEV' THEN 2
        WHEN 'MAR' THEN 3
        WHEN 'ABR' THEN 4
        WHEN 'MAI' THEN 5
    END;
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| mes | STRING | Mês (formato abreviado PT-BR) |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| qtd_vendedores_ativos | BIGINT | Vendedores com vendas no mês |

**Validação**: Comparar total_vendas com aba "Base Grafico" (colunas 6-7, linhas 13-18)

---

### View 4: vw_vendas_por_secao

**Propósito**: Agregar total de vendas por seção/categoria de produto.

**SQL de Criação**:

```sql
CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_secao AS
SELECT 
    secao,
    SUM(valor_vendas) AS total_vendas,
    COUNT(*) AS qtd_transacoes,
    ROUND(AVG(valor_vendas), 2) AS ticket_medio
FROM main.vendas_regionais.tb_vendas_base
GROUP BY secao
ORDER BY total_vendas DESC;
```

**Schema de Saída**:
| Coluna | Tipo | Descrição |
|--------|------|-----------|
| secao | STRING | Seção/categoria do produto |
| total_vendas | DECIMAL | Soma de todas as vendas |
| qtd_transacoes | BIGINT | Quantidade de transações |
| ticket_medio | DECIMAL | Valor médio por transação |

**Validação**: Comparar total_vendas com aba "Base Grafico" (colunas 9-10, linhas 13-21)

---

## Fluxo de Implementação

### 1. Inicialização e Logging

```python
# Importar LogControl
%run "/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/sdd/features/error_handler_logging/src/logger_control"

# Configurar logger
logger = LogControl(
    logger_name="vendas_semantic_layer",
    tbl_name="main.vendas_regionais.tb_logs_semantic"
)

logger.log_info("=== INICIANDO CRIAÇÃO DAS VIEWS SEMÂNTICAS ===")
```

### 2. Validar Tabela Base Existe

```python
try:
    logger.log_info("Validando existência da tabela base")
    
    # Verificar se tabela existe
    df_base = spark.table("main.vendas_regionais.tb_vendas_base")
    count_base = df_base.count()
    
    logger.log_success(f"Tabela base encontrada com {count_base} registros")
    
except Exception as e:
    logger.log_error("Tabela base não encontrada. Execute vendas_base_ingestion primeiro.")
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 3. Criar Views

```python
views = {
    "vw_vendas_por_vendedor": """
        CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_vendedor AS
        SELECT 
            vendedor,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            AVG(valor_vendas) AS ticket_medio
        FROM main.vendas_regionais.tb_vendas_base
        GROUP BY vendedor
        ORDER BY total_vendas DESC
    """,
    "vw_vendas_por_regiao": """
        CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_regiao AS
        SELECT 
            regiao,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            COUNT(DISTINCT vendedor) AS qtd_vendedores
        FROM main.vendas_regionais.tb_vendas_base
        GROUP BY regiao
        ORDER BY total_vendas DESC
    """,
    "vw_vendas_por_mes": """
        CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_mes AS
        SELECT 
            mes,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            COUNT(DISTINCT vendedor) AS qtd_vendedores_ativos
        FROM main.vendas_regionais.tb_vendas_base
        GROUP BY mes
        ORDER BY 
            CASE mes
                WHEN 'JAN' THEN 1
                WHEN 'FEV' THEN 2
                WHEN 'MAR' THEN 3
                WHEN 'ABR' THEN 4
                WHEN 'MAI' THEN 5
            END
    """,
    "vw_vendas_por_secao": """
        CREATE OR REPLACE VIEW main.vendas_regionais.vw_vendas_por_secao AS
        SELECT 
            secao,
            SUM(valor_vendas) AS total_vendas,
            COUNT(*) AS qtd_transacoes,
            ROUND(AVG(valor_vendas), 2) AS ticket_medio
        FROM main.vendas_regionais.tb_vendas_base
        GROUP BY secao
        ORDER BY total_vendas DESC
    """
}

for view_name, view_sql in views.items():
    try:
        logger.log_info(f"Criando view: {view_name}")
        spark.sql(view_sql)
        logger.log_success(f"View {view_name} criada com sucesso")
    except Exception as e:
        logger.log_error(f"Erro ao criar view {view_name}")
        logger.error_handler(e, debug_write_mode=True)
        raise
```

### 4. Validar Views Criadas

```python
try:
    logger.log_info("Validando views criadas")
    
    for view_name in views.keys():
        full_view_name = f"main.vendas_regionais.{view_name}"
        df_view = spark.table(full_view_name)
        count = df_view.count()
        logger.log_info(f"{view_name}: {count} registros")
    
    logger.log_success("Todas as views validadas com sucesso")
    
except Exception as e:
    logger.log_error("Erro durante validação das views")
    logger.error_handler(e, debug_write_mode=True)
    raise
```

### 5. Validar Contra Excel (Base Grafico)

```python
try:
    logger.log_info("Validando agregações contra aba Base Grafico")
    
    import pandas as pd
    excel_path = "/Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/arquivos/VendasRegionaisVBA.xlsm"
    df_grafico = pd.read_excel(excel_path, sheet_name='Base Grafico', header=None)
    
    # Validar vendas por vendedor
    # (Comparações detalhadas aqui)
    
    logger.log_success("Validações contra Excel concluídas")
    
except Exception as e:
    logger.log_warning(f"Não foi possível validar contra Excel: {str(e)}")
```

## Tratamento de Erros

### Exceções Esperadas

1. **AnalysisException**: Tabela base não existe
   * Ação: Logar erro e instruir execução de vendas_base_ingestion primeiro

2. **ParseException**: Erro de sintaxe SQL
   * Ação: Logar erro com SQL completo, interromper execução

3. **Validação Falhou**: Totais não conferem com Excel
   * Ação: Logar warning (não interromper), mostrar divergências

## Métricas de Validação

### Totais Esperados (Base Grafico)

* **Total Geral**: ~R$ 225.926,23 (soma de todas as vendas)
* **Por Vendedor**: 8 vendedores com totais variando de ~R$ 10.214 a ~R$ 40.064
* **Por Região**: 4 regiões (Norte, Sul, Sudeste, Nordeste)
* **Por Mês**: 5 meses (JAN a MAI)
* **Por Seção**: 8 seções de produtos

### Tolerância

* **Arredondamento**: ± 0.01 (1 centavo) por agregação
* **Percentual**: < 0.001% de divergência aceitável

## Performance

* **Complexidade**: O(n) para cada GROUP BY (n = 90 registros)
* **Tempo Esperado**: < 1 segundo por view
* **Cache**: Não necessário devido ao baixo volume

## Dependências

* Tabela `main.vendas_regionais.tb_vendas_base` (feature vendas_base_ingestion)
* LogControl (feature error_handler_logging)
* PySpark SQL

## Testes Requeridos

1. Teste de criação das 4 views
2. Teste de consultabilidade (SELECT * funciona)
3. Teste de contagem de registros (>0 em cada view)
4. Teste de validação de totais vs. Excel
5. Teste de LogControl integração
