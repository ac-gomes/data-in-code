# Plan: Vendas Base Ingestion

## Propósito

Criar uma feature de ingestão de dados que leia arquivos CSV sintéticos da pasta `data/` e persista os dados em uma tabela Delta no formato otimizado para análise com PySpark no Unity Catalog.

## Contexto de Negócio

A pasta `data/` contém arquivos CSV com dados transacionais sintéticos de vendas regionais que servem como fonte primária para análises e dashboards. Os arquivos contêm o conjunto de dados com informações de:

* Datas de venda
* Região geográfica
* Vendedores e seus códigos
* Seção de produtos
* Valores de vendas
* Mês da transação (abreviado)

## Regras de Negócio

### Fonte de Dados

* **Pasta**: `/Workspace/Users/data.in.code/data-in-code/vendas_regionais/data/`
* **Formato**: Arquivos CSV (.csv)
* **Volume**: Aproximadamente 1000 registros transacionais (dados sintéticos)
* **Período**: Janeiro a Dezembro de 2018

### Estrutura dos Dados

Os arquivos CSV possuem as seguintes colunas:

1. **Data da Venda** (string/date): Data da transação de venda
2. **Mês** (string): Mês da venda em formato abreviado (JAN, FEV, MAR, ABR, MAI, JUN, JUL, AGO, SET, OUT, NOV, DEZ)
3. **Região** (string): Região geográfica da venda (Norte, Sul, Sudeste, Nordeste)
4. **Vendedor** (string): Nome do vendedor responsável pela venda
5. **Código Vendedor** (integer): Código único do vendedor (1-8)
6. **Seção** (string): Seção/categoria do produto vendido
7. **Vendas** (decimal): Valor monetário da venda

### Qualidade de Dados

* Todos os campos são obrigatórios (não devem existir nulos nas 7 colunas principais)
* Valores de vendas devem ser positivos
* Datas devem estar no formato válido (YYYY-MM-DD)
* Regiões devem estar no conjunto: {Norte, Sul, Sudeste, Nordeste}
* Códigos de vendedor devem estar no range: 1-8

## Estratégia de Persistência

### Tabela Delta de Destino

* **Catalog**: `workspace` (Unity Catalog)
* **Schema**: `vendas_regionais`
* **Table Name**: `vendas_base`
* **Full Qualified Name**: `workspace.vendas_regionais.vendas_base`
* **Formato**: Delta Lake (otimizado para Databricks/PySpark)
* **Modo de Escrita**: Overwrite (por se tratar de carga completa)
* **Particionamento**: Não necessário devido ao baixo volume de dados

### Justificativa Técnica

1. **Unity Catalog**: Governança centralizada, controle de acesso, auditoria
2. **Delta Lake**: Fornece ACID transactions, time travel, e schema enforcement
3. **PySpark**: Permite processamento escalável e integração com o ecossistema Databricks
4. **Overwrite**: Garante consistência total com a fonte, evitando duplicações

## Arquitetura de Processamento (v3.1.0)

### Fluxo Simplificado

```
1. Ler CSV da pasta data/
   ↓
2. Converter IMEDIATAMENTE para PySpark DataFrame (via pandas intermediário)
   ↓
3. Limpeza básica (remover Unnamed, trim strings)
   ↓
4. Tipagem e derivações (date, ano, mês, data_carga)
   ↓
5. Validações de qualidade (nulos, ranges, valores válidos)
   ↓
6. Escrever DIRETAMENTE na tabela Delta
   ↓
7. Validações pós-carga (lendo DA TABELA)
   ↓
8. Métricas finais
```

### Características Principais

* ✅ **Conversão imediata**: pandas → PySpark logo após leitura (evita overhead de memória)
* ✅ **Processamento distribuído**: Usa PySpark desde o início
* ✅ **Fluxo direto**: Sem exposição de DataFrame para outros notebooks
* ✅ **Validações da tabela**: Lê da tabela Delta para validações finais (evita duplicação de dados)
* ✅ **Escalável**: Suporta múltiplos arquivos CSV via union

## Logging e Tratamento de Erros

### Padrão LogControl

* **Obrigatório**: Uso da classe `LogControl` para todos os logs
* **Tabela de Logs**: `main.vendas_regionais.tb_logs_ingestion`
* **Rastreabilidade**: Captura de exceções com stack trace completo
* **Níveis de Log**:
  * INFO: Início da ingestão, validações de schema, progresso
  * SUCCESS: Conclusão bem-sucedida da carga, validações OK
  * WARNING: Dados inválidos identificados (nulos, ranges)
  * ERROR: Falhas críticas na leitura ou escrita

### Tratamento de Exceções

* Captura de erros de leitura dos CSV (arquivo não encontrado, formato inválido)
* Captura de erros de schema (tipos incompatíveis)
* Captura de erros de escrita Delta (permissões, Unity Catalog)
* Persistência de logs em tabela Delta para auditoria

## Anti-Patterns a Evitar

* ❌ Manter dados grandes em pandas por muito tempo (converter imediatamente para Spark)
* ❌ Loops sobre DataFrames (usar operações vetorizadas)
* ❌ UDFs desnecessárias (priorizar funções nativas do Spark)
* ❌ Logging customizado (sempre usar `LogControl`)
* ❌ Ignorar validações de schema e qualidade
* ❌ Expor DataFrame para outros notebooks (usar tabela Delta como fonte de verdade)
* ❌ Validar em memória quando já temos dados persistidos (ler da tabela)

## Dependências

* **Biblioteca**: pandas (apenas para leitura inicial dos CSV)
* **Biblioteca**: PySpark (processamento principal)
* **Feature**: error_handler_logging (LogControl)
* **Compute**: Databricks Serverless (ou cluster com Unity Catalog habilitado)
* **Unity Catalog**: Workspace catalog ativo

## Schema Enriquecido

Além das 7 colunas originais do CSV, o pipeline adiciona:

* **ano** (integer): Extraído de `data_venda` para facilitar análises temporais
* **mes** (integer): Número do mês (1-12) extraído de `data_venda`
* **data_carga** (timestamp): Timestamp da ingestão para auditoria

**Total**: 10 colunas na tabela Delta final

## Próximos Passos

1. ✅ Criar especificação técnica detalhada (`spec.md`) - ATUALIZADO
2. ✅ Definir tarefas granulares de implementação (`tasks.md`) - ATUALIZADO
3. ✅ Implementar código fonte com LogControl integrado - CONCLUÍDO
4. ✅ Validar carga completa e qualidade dos dados - CONCLUÍDO
5. 🔄 Criar testes automatizados (próxima fase)
