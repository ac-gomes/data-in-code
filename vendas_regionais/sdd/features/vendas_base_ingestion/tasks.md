# Tasks: Vendas Base Ingestion

## Status Geral (v3.1.0)
* Total de Tasks: 15 grupos principais
* Concluídas: ✅ **TODAS as tasks de implementação**
* Em Progresso: 0
* Pendentes: Testes automatizados (próxima fase)

**Última Atualização**: 2026-04-19 - Tasks atualizadas para refletir implementação v3.1.0

---

## Tasks de Implementação

### 1. Setup e Configuração
* [x] **Task 1.1**: Criar estrutura de diretórios `src/`
* [x] **Task 1.2**: Criar notebook `ingest_vendas_base`
* [x] **Task 1.3**: Importar LogControl e configurar logger com nome "vendas_base_ingestion"
  * Implementado: `%run ../../error_handler_logging/src/logger_control`
  * Tabela de logs: `main.vendas_regionais.tb_logs_ingestion`
* [x] **Task 1.4**: Criar schema Unity Catalog se não existir
  * Implementado: `workspace.vendas_regionais` (Unity Catalog)
* [x] **Task 1.5**: Definir parâmetros de configuração (DATA_DIR, TARGET_CATALOG, etc.)

### 2. Leitura dos Arquivos CSV
* [x] **Task 2.1**: Implementar leitura de arquivos CSV da pasta data/ com pandas
  * Path: `/Workspace/Users/data.in.code/data-in-code/vendas_regionais/data/`
  * Suporte a múltiplos arquivos CSV
* [x] **Task 2.2**: Validar que a pasta existe e contém arquivos CSV
  * Implementado: `os.listdir()` + filtro `.endswith('.csv')`
* [x] **Task 2.3**: Logar início da leitura
  * Implementado: `logger.log_info()`
* [x] **Task 2.4**: Capturar exceções de leitura
  * Implementado: `try/except` com `logger.error_handler()`
* [x] **Task 2.5**: Converter IMEDIATAMENTE pandas para PySpark DataFrame
  * Implementado: `spark.createDataFrame(df_temp_pandas)` logo após leitura
  * Evita overhead de memória
* [x] **Task 2.6**: Union de múltiplos arquivos CSV em um único DataFrame Spark
  * Implementado: Loop com `.union()`

### 3. Limpeza e Transformação de Dados (PySpark)
* [x] **Task 3.1**: Remover colunas "Unnamed" do DataFrame
  * Implementado: List comprehension + `.select()`
* [x] **Task 3.2**: Remover linhas completamente vazias
  * Implementado: Filtro com condição OR de `.isNotNull()`
* [x] **Task 3.3**: Renomear colunas para snake_case
  * Implementado: Dicionário de mapeamento + `.withColumnRenamed()`
  * Mapeamento: Data da Venda → data_venda, Região → regiao, etc.
* [x] **Task 3.4**: Trim de strings em todas as colunas string
  * Implementado: Loop sobre colunas + `F.trim()`
* [x] **Task 3.5**: Logar schema do DataFrame após transformações
  * Implementado: `logger.log_success()` + contagem

### 4. Tipagem e Derivações (PySpark)
* [x] **Task 4.1**: Converter data_venda para DateType
  * Implementado: `F.to_date(F.col("data_venda"))`
* [x] **Task 4.2**: Extrair coluna ano (IntegerType)
  * Implementado: `F.year(F.col("data_venda"))`
* [x] **Task 4.3**: Extrair coluna mes (IntegerType)
  * Implementado: `F.month(F.col("data_venda"))`
* [x] **Task 4.4**: Adicionar coluna `data_carga` com timestamp atual
  * Implementado: `F.current_timestamp()`
* [x] **Task 4.5**: Logar schema final após tipagem
  * Implementado: `logger.log_success()` + `df_clean.printSchema()`

### 5. Validações de Qualidade (PySpark)
* [x] **Task 5.1**: Validar ausência de nulos nas colunas obrigatórias
  * Implementado: Loop sobre colunas + `.filter(isNull()).count()`
* [x] **Task 5.2**: Validar que valores de vendas são positivos (> 0)
  * Implementado: `.filter(valor_vendas <= 0).count()`
* [x] **Task 5.3**: Validar que regiões estão no conjunto válido
  * Implementado: `.filter(~col.isin(valid_regions)).count()`
* [x] **Task 5.4**: Validar range de codigo_vendedor (1-8)
  * Implementado: `.filter((col < 1) | (col > 8)).count()`
* [x] **Task 5.5**: Validar duplicatas
  * Implementado: `.groupBy().count().filter(count > 1)`
* [x] **Task 5.6**: Logar warnings caso dados inválidos sejam encontrados
  * Implementado: `logger.log_warning()` para cada validação com issues
* [x] **Task 5.7**: Logar sucesso se todas validações passarem
  * Implementado: `logger.log_success("Validações concluídas")`

### 6. Persistência em Delta Table (Unity Catalog)
* [x] **Task 6.1**: Criar schema Unity Catalog se não existir
  * Implementado: `spark.sql("CREATE DATABASE IF NOT EXISTS workspace.vendas_regionais")`
* [x] **Task 6.2**: Implementar escrita no formato Delta com mode="overwrite"
  * Implementado: `.write.format("delta").mode("overwrite")`
* [x] **Task 6.3**: Habilitar opção `overwriteSchema=true`
  * Implementado: `.option("overwriteSchema", "true")`
* [x] **Task 6.4**: Salvar como tabela no Unity Catalog
  * Implementado: `.saveAsTable("workspace.vendas_regionais.vendas_base")`
* [x] **Task 6.5**: Logar quantidade de registros persistidos
  * Implementado: `logger.log_success()`
* [x] **Task 6.6**: Capturar exceções de escrita
  * Implementado: `try/except` com `logger.error_handler()`

### 7. Validações Pós-Carga (Lendo da Tabela)
* [x] **Task 7.1**: Ler dados da tabela Delta para validação
  * Implementado: `spark.table(FULL_TABLE_NAME)`
  * ✅ Evita duplicação de dados (lê da fonte de verdade)
* [x] **Task 7.2**: Validar contagem de registros escritos
  * Implementado: `df_delta.count()`
* [x] **Task 7.3**: Exibir schema da tabela Delta
  * Implementado: `df_delta.printSchema()`
* [x] **Task 7.4**: Exibir amostra de registros (10 primeiros)
  * Implementado: `display(df_delta.limit(10))`
* [x] **Task 7.5**: Exibir agregações de sanidade por região
  * Implementado: `.groupBy("regiao").agg(count, sum, avg)` + display
* [x] **Task 7.6**: Logar sucesso das validações pós-carga
  * Implementado: `logger.log_success()`

### 8. Métricas Finais
* [x] **Task 8.1**: Exibir resumo da ingestão
  * Implementado: Log estruturado com:
    * Fonte (pasta data/)
    * Tabela destino (workspace.vendas_regionais.vendas_base)
    * Modo de escrita (overwrite)
    * Registros na tabela
    * Tipo de processamento (PySpark distribuído)
    * Timestamp da execução
* [x] **Task 8.2**: Logar conclusão bem-sucedida
  * Implementado: `logger.log_success("=== INGESTÃO CONCLUÍDA COM SUCESSO ===")`

### 9. Documentação
* [x] **Task 9.1**: Atualizar header do notebook com versão e descrição
  * Implementado: Markdown cell com versão 3.1.0, arquitetura, fluxo simplificado
* [x] **Task 9.2**: Atualizar plan.md com estado atual
  * Implementado: 2026-04-19
* [x] **Task 9.3**: Atualizar spec.md com implementação real
  * Implementado: 2026-04-19
* [x] **Task 9.4**: Atualizar tasks.md com estado de conclusão
  * Implementado: 2026-04-19

---

## Testes Automatizados (Próxima Fase)

### 10. Criação de Testes
* [ ] **Task 10.1**: Criar notebook `src/tests/nb_test_vendas_base_ingestion`
* [ ] **Task 10.2**: Implementar teste: leitura bem-sucedida dos CSV
* [ ] **Task 10.3**: Implementar teste: schema do DataFrame está correto
* [ ] **Task 10.4**: Implementar teste: contagem de registros = 1000 (ou conforme arquivo)
* [ ] **Task 10.5**: Implementar teste: tabela Delta existe e é consultável
* [ ] **Task 10.6**: Implementar teste: validações de qualidade passam
* [ ] **Task 10.7**: Implementar teste: LogControl está funcionando corretamente
* [ ] **Task 10.8**: Executar todos os testes e documentar resultados

---

## Validação Final

### Critérios de Aceitação
- [x] Notebook `ingest_vendas_base` existe e executa sem erros
- [x] Tabela `workspace.vendas_regionais.vendas_base` existe com ~1000 registros
- [x] LogControl está integrado e logs são persistidos corretamente
- [x] Código segue padrões de qualidade (validações, tratamento de erros)
- [x] Dados limpos e transformados corretamente
- [x] Processo executa end-to-end sem erros
- [x] Conversão imediata pandas → PySpark implementada
- [x] Validações pós-carga lendo da tabela (evita duplicação)
- [x] Unity Catalog implementado corretamente
- [x] Documentação atualizada (plan, spec, tasks)
- [ ] Testes automatizados implementados (100%) - **PENDENTE: PRÓXIMA FASE**

---

## Implementação Atual (v3.1.0)

### ✅ Concluído com Sucesso

#### Arquitetura
* ✅ Leitura de arquivos CSV da pasta data/ (não Excel)
* ✅ Conversão IMEDIATA pandas → PySpark (evita overhead)
* ✅ Processamento distribuído desde o início
* ✅ Fluxo simplificado: CSV → PySpark → Tipagem → Delta → Validações
* ✅ Sem exposição de DataFrame para outros notebooks
* ✅ Validações pós-carga lendo DA TABELA (evita duplicação)

#### Persistência
* ✅ Unity Catalog: `workspace.vendas_regionais.vendas_base`
* ✅ Schema criado automaticamente se não existir
* ✅ Delta Lake com overwriteSchema habilitado
* ✅ Modo overwrite (carga completa)

#### Qualidade de Dados
* ✅ Limpeza de colunas Unnamed
* ✅ Remoção de linhas vazias
* ✅ Renomeação para snake_case
* ✅ Trim de strings
* ✅ Tipagem correta (date, long, double, int, timestamp)
* ✅ Derivações (ano, mes, data_carga)
* ✅ Validações abrangentes (nulos, ranges, valores válidos, duplicatas)

#### Logging e Observabilidade
* ✅ LogControl integrado desde o início
* ✅ Logs estruturados com INFO/SUCCESS/WARNING/ERROR
* ✅ Tratamento de exceções com stack trace
* ✅ Métricas finais com resumo completo
* ✅ Persistência de logs em tabela Delta

#### Documentação
* ✅ Header do notebook atualizado (v3.1.0)
* ✅ plan.md atualizado para CSV + Unity Catalog
* ✅ spec.md atualizado com fluxo simplificado
* ✅ tasks.md atualizado com estado real

### Mudanças da Versão Anterior (v3.0.0 → v3.1.0)

#### Removido
* ❌ Célula redundante "Conversão para Spark DataFrame"
* ❌ Exposição de DataFrame `spark_df` para outros notebooks
* ❌ Célula 25 que disponibilizava DataFrame via `%run`

#### Adicionado
* ✅ Validações pós-carga lendo DA TABELA (nova célula)
* ✅ Evita duplicação de dados (valida fonte de verdade)

#### Atualizado
* 🔄 Fluxo simplificado e direto
* 🔄 Documentação alinhada com implementação
* 🔄 Unity Catalog padronizado (workspace)

### Estatísticas de Execução

* **Volume de Dados**: ~1000 registros (dados sintéticos)
* **Arquivos CSV**: 1 arquivo na pasta data/
* **Tempo de Execução**: < 1 minuto
* **Tabela de Destino**: `workspace.vendas_regionais.vendas_base`
* **Colunas**: 10 (7 originais + 3 derivadas)
* **Formato**: Delta Lake
* **Compute**: Databricks Serverless

---

## Próximos Passos Recomendados

1. 🔄 **Criar testes automatizados** (Task 10.x)
   * Teste de leitura dos CSV
   * Teste de schema
   * Teste de contagem de registros
   * Teste de validações de qualidade
   * Teste de integração com LogControl

2. 🔄 **Monitoramento contínuo**
   * Alertas para falhas de ingestão
   * Dashboard de métricas de qualidade
   * Análise de logs históricos

3. 🔄 **Otimizações futuras** (se volume crescer)
   * Particionamento por ano/mês
   * Z-ordering por região
   * Delta Lake optimization commands
