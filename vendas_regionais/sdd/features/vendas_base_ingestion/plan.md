# Plan: Vendas Base Ingestion

## Propósito

Criar uma feature de ingestão de dados que leia a aba "Base" do arquivo Excel `VendasRegionaisVBA.xlsm` e persista os dados em uma tabela Delta no formato otimizado para análise com PySpark.

## Contexto de Negócio

O arquivo `VendasRegionaisVBA.xlsm` contém dados transacionais de vendas regionais que servem como fonte primária para análises e dashboards. A aba "Base" contém o conjunto de dados bruto com informações de:

* Datas de venda
* Região geográfica
* Vendedores e seus códigos
* Seção de produtos
* Valores de vendas
* Mês da transação

## Regras de Negócio

### Fonte de Dados

* **Arquivo**: `/Workspace/Users/data.in.code@gmail.com/data-in-code/vendas_regionais/arquivos/VendasRegionaisVBA.xlsm`
* **Aba**: `Base`
* **Volume**: Aproximadamente 90 registros transacionais
* **Período**: Janeiro a Maio de 2018

### Estrutura dos Dados

A aba "Base" possui as seguintes colunas relevantes (colunas "Unnamed" devem ser ignoradas):

1. **Data da Venda** (datetime): Data da transação de venda
2. **Região** (string): Região geográfica da venda (Norte, Sul, Sudeste, Nordeste)
3. **Vendedor** (string): Nome do vendedor responsável pela venda
4. **Código Vendedor** (integer): Código único do vendedor
5. **Seção** (string): Seção/categoria do produto vendido
6. **Vendas** (decimal): Valor monetário da venda
7. **Mês** (string): Mês da venda em formato abreviado (ABR, MAI, JAN, FEV, MAR)

### Qualidade de Dados

* Todos os campos são obrigatórios (não devem existir nulos nas 7 colunas principais)
* Valores de vendas devem ser positivos
* Datas devem estar no formato válido
* Regiões devem estar no conjunto: {Norte, Sul, Sudeste, Nordeste}
* Meses devem estar no conjunto: {JAN, FEV, MAR, ABR, MAI}

## Estratégia de Persistência

### Tabela Delta de Destino

* **Nome da Tabela**: A ser definido na especificação técnica
* **Formato**: Delta Lake (otimizado para Databricks/PySpark)
* **Modo de Escrita**: Overwrite (por se tratar de carga completa de arquivo Excel estático)
* **Particionamento**: Não necessário devido ao baixo volume de dados

### Justificativa Técnica

1. **Delta Lake**: Fornece ACID transactions, time travel, e schema enforcement
2. **PySpark**: Permite processamento escalável e integração com o ecossistema Databricks
3. **Overwrite**: Garante consistência total com a fonte Excel, evitando duplicações

## Logging e Tratamento de Erros

### Padrão LogControl

* **Obrigatório**: Uso da classe `LogControl` para todos os logs
* **Rastreabilidade**: Captura de exceções com stack trace completo
* **Níveis de Log**:
  * INFO: Início da ingestão, validações de schema
  * SUCCESS: Conclusão bem-sucedida da carga
  * WARNING: Dados inválidos identificados (se aplicável)
  * ERROR: Falhas críticas na leitura ou escrita

### Tratamento de Exceções

* Captura de erros de leitura do Excel (arquivo não encontrado, aba inválida)
* Captura de erros de schema (tipos incompatíveis)
* Captura de erros de escrita Delta (permissões, espaço em disco)
* Persistência de logs em tabela Delta para auditoria

## Anti-Patterns a Evitar

* ❌ Uso de `toPandas()` para grandes volumes (não aplicável aqui devido ao baixo volume, mas evitar como prática)
* ❌ Loops sobre DataFrames (usar operações vetorizadas)
* ❌ UDFs desnecessárias (priorizar funções nativas do Spark)
* ❌ Logging customizado (sempre usar `LogControl`)
* ❌ Ignorar validações de schema e qualidade

## Dependências

* **Biblioteca**: pandas (para leitura do Excel)
* **Biblioteca**: openpyxl (engine para arquivos .xlsm)
* **Feature**: error_handler_logging (LogControl)
* **Compute**: Databricks runtime com suporte a PySpark

## Próximos Passos

1. Criar especificação técnica detalhada (`spec.md`)
2. Definir tarefas granulares de implementação (`tasks.md`)
3. Implementar código fonte com testes automatizados
4. Validar carga completa e qualidade dos dados
