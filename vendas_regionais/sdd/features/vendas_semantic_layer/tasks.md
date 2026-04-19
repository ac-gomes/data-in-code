# Tasks: Vendas Semantic Layer

**Versão**: 2.0.0 (refatorada para v3.1.0 do ingest_vendas_base)  
**Última Atualização**: 2026-04-19

## Status Geral
* Total de Tasks: 13 grupos principais (77 tasks individuais)
* Concluídas: 12 grupos (71 tasks)
* Em Progresso: 0
* Pendentes: 1 grupo (6 tasks) - Testes automatizados

## MUDANÇAS ARQUITETURAIS (v2.0.0)

### O Que Mudou

**ANTES (v1.0)**:
* Recebia DataFrame via `%run ./ingest_vendas_base`
* Criava views persistidas com CREATE OR REPLACE VIEW
* Tabela: main.vendas_regionais.tb_vendas_base
* Depende de execução de outro notebook

**AGORA (v2.0)**:
* Lê DIRETAMENTE da tabela Delta com `spark.table()`
* Cria temp views com CREATE OR REPLACE TEMP VIEW
* Tabela: workspace.vendas_regionais.vendas_base (Unity Catalog)
* Execução independente (não precisa executar outro notebook)

### Tasks de Refatoração (Todas Concluídas)
* [x] **Refactor 1**: Remover célula com `%run ./ingest_vendas_base`
* [x] **Refactor 2**: Substituir lógica para ler diretamente da tabela Delta
* [x] **Refactor 3**: Adicionar validação de existência da tabela
* [x] **Refactor 4**: Atualizar SQL de CREATE OR REPLACE VIEW para CREATE OR REPLACE TEMP VIEW
* [x] **Refactor 5**: Atualizar header do notebook com nova arquitetura
* [x] **Refactor 6**: Atualizar sumário final com fonte de dados correta
* [x] **Refactor 7**: Atualizar documentação SDD (plan.md, spec.md, tasks.md)

---

## Tasks de Implementação

### 1. Setup e Configuração
* [x] **Task 1.1**: Verificar que feature vendas_base_ingestion (v3.1.0) foi executada
* [x] **Task 1.2**: Confirmar que tabela `workspace.vendas_regionais.vendas_base` existe
* [x] **Task 1.3**: Criar notebook `nb_create_semantic_views`
* [x] **Task 1.4**: Importar LogControl e configurar logger "vendas_semantic_layer"
* [x] **Task 1.5**: Configurar tabela de logs: main.vendas_regionais.tb_logs_semantic

### 2. Leitura da Tabela Delta e Validação
* [x] **Task 2.1**: Ler tabela Delta com `spark.table("workspace.vendas_regionais.vendas_base")`
* [x] **Task 2.2**: Validar que a tabela existe (capturar AnalysisException)
* [x] **Task 2.3**: Validar que a tabela tem dados (count > 0)
* [x] **Task 2.4**: Criar temp view base `vendas_base_temp` com createOrReplaceTempView()
* [x] **Task 2.5**: Exibir amostra dos dados (5 primeiros registros)
* [x] **Task 2.6**: Logar estatísticas: total de registros, fonte da tabela
* [x] **Task 2.7**: Validar schema da tabela (printSchema)
* [x] **Task 2.8**: Calcular total de vendas e período de dados

### 3. Criação da View: vw_vendas_por_vendedor
* [x] **Task 3.1**: Escrever SQL CREATE OR REPLACE TEMP VIEW para vw_vendas_por_vendedor
* [x] **Task 3.2**: Adicionar colunas: vendedor, total_vendas, qtd_transacoes, ticket_medio
* [x] **Task 3.3**: Implementar GROUP BY vendedor
* [x] **Task 3.4**: Ordenar por total_vendas DESC
* [x] **Task 3.5**: Executar SQL com spark.sql()
* [x] **Task 3.6**: Logar sucesso com log_success

### 4. Criação da View: vw_vendas_por_regiao
* [x] **Task 4.1**: Escrever SQL CREATE OR REPLACE TEMP VIEW para vw_vendas_por_regiao
* [x] **Task 4.2**: Adicionar colunas: regiao, total_vendas, qtd_transacoes, qtd_vendedores
* [x] **Task 4.3**: Implementar GROUP BY regiao com COUNT DISTINCT vendedor
* [x] **Task 4.4**: Ordenar por total_vendas DESC
* [x] **Task 4.5**: Executar SQL com spark.sql()
* [x] **Task 4.6**: Logar sucesso com log_success

### 5. Criação da View: vw_vendas_por_mes
* [x] **Task 5.1**: Escrever SQL CREATE OR REPLACE TEMP VIEW para vw_vendas_por_mes
* [x] **Task 5.2**: Adicionar colunas: mes, total_vendas, qtd_transacoes, qtd_vendedores_ativos
* [x] **Task 5.3**: Implementar GROUP BY mes
* [x] **Task 5.4**: Implementar ORDER BY mes ASC (ordenação numérica 1-12)
* [x] **Task 5.5**: Executar SQL com spark.sql()
* [x] **Task 5.6**: Logar sucesso com log_success

### 6. Criação da View: vw_vendas_por_secao
* [x] **Task 6.1**: Escrever SQL CREATE OR REPLACE TEMP VIEW para vw_vendas_por_secao
* [x] **Task 6.2**: Adicionar colunas: secao, total_vendas, qtd_transacoes, ticket_medio
* [x] **Task 6.3**: Implementar GROUP BY secao
* [x] **Task 6.4**: Ordenar por total_vendas DESC
* [x] **Task 6.5**: Executar SQL com spark.sql()
* [x] **Task 6.6**: Logar sucesso com log_success

### 7. Validações das Views
* [x] **Task 7.1**: Validar que todas as 4 temp views existem (SELECT * funciona)
* [x] **Task 7.2**: Executar SELECT COUNT(*) em cada view para garantir que retornam dados
* [x] **Task 7.3**: Validar schema de cada view (colunas e tipos)
* [x] **Task 7.4**: Logar estatísticas de cada view (qtd registros)
* [x] **Task 7.5**: Capturar exceções com error_handler()

### 8. Exibição de Resultados
* [x] **Task 8.1**: Executar SELECT * em vw_vendas_por_vendedor e exibir com display()
* [x] **Task 8.2**: Executar SELECT * em vw_vendas_por_regiao e exibir com display()
* [x] **Task 8.3**: Executar SELECT * em vw_vendas_por_mes e exibir com display()
* [x] **Task 8.4**: Executar SELECT * em vw_vendas_por_secao e exibir com display()

### 9. Testes Automatizados
* [ ] **Task 9.1**: Criar arquivo `src/tests/nb_test_semantic_views`
* [ ] **Task 9.2**: Implementar teste: tabela Delta existe e tem dados
* [ ] **Task 9.3**: Implementar teste: todas as 4 temp views são criadas sem erros
* [ ] **Task 9.4**: Implementar teste: views retornam dados (COUNT > 0)
* [ ] **Task 9.5**: Implementar teste: schemas das views estão corretos
* [ ] **Task 9.6**: Implementar teste: LogControl funcionando

### 10. Documentação
* [x] **Task 10.1**: Atualizar README.md em src/ com instruções de uso das temp views
* [x] **Task 10.2**: Documentar queries de exemplo para cada view
* [x] **Task 10.3**: Adicionar comentários no código sobre regras de negócio
* [x] **Task 10.4**: Atualizar plan.md com arquitetura v2.0.0
* [x] **Task 10.5**: Atualizar spec.md com código completo v2.0.0
* [x] **Task 10.6**: Atualizar tasks.md com estado real pós-refatoração

### 11. Tratamento de Erros
* [x] **Task 11.1**: Capturar AnalysisException (tabela base não existe)
* [x] **Task 11.2**: Capturar ValueError (tabela base vazia)
* [x] **Task 11.3**: Capturar ParseException (erro de sintaxe SQL)
* [x] **Task 11.4**: Capturar exceções gerais durante criação das views
* [x] **Task 11.5**: Garantir que todos os erros são logados com error_handler()
* [x] **Task 11.6**: Re-lançar exceções críticas após logging

### 12. Validação de Integração
* [x] **Task 12.1**: Validar que a tabela Delta existe antes de criar views
* [x] **Task 12.2**: Validar que a leitura da tabela Delta funciona
* [x] **Task 12.3**: Validar que temp view base é criada corretamente
* [x] **Task 12.4**: Validar que temp views agregadas usam a temp view base

### 13. Validação Final
* [x] **Task 13.1**: Executar processo completo de ponta a ponta
* [x] **Task 13.2**: Confirmar que todas as 4 temp views estão acessíveis via SQL
* [x] **Task 13.3**: Validar que notebook pode ser executado de forma independente
* [x] **Task 13.4**: Validar que não há dependência de %run de outro notebook
* [x] **Task 13.5**: Atualizar tasks.md marcando tasks implementadas como concluídas [x]

---

## Validação Final

### Critérios de Aceitação
- [x] Notebook `nb_create_semantic_views` existe e executa sem erros
- [x] Lê diretamente da tabela Delta `workspace.vendas_regionais.vendas_base`
- [x] Não usa `%run` para receber DataFrame de outro notebook
- [x] Valida que a tabela Delta existe e tem dados antes de prosseguir
- [x] Cria temp view base `vendas_base_temp` com createOrReplaceTempView()
- [x] 4 temp views criadas: vw_vendas_por_vendedor, vw_vendas_por_regiao, vw_vendas_por_mes, vw_vendas_por_secao
- [x] Todas as temp views usam CREATE OR REPLACE TEMP VIEW (não persistidas)
- [ ] Todos os testes em `src/tests/nb_test_semantic_views` passam (100%) - **PENDENTE**
- [x] LogControl integrado e logs persistidos corretamente
- [x] Queries de exemplo documentadas no notebook
- [x] Implementação segue 100% o spec.md v2.0.0
- [x] Documentação SDD atualizada (plan.md, spec.md, tasks.md)

---

## Observações de Implementação

### Concluído com Sucesso ✅
* **Refatoração Completa (v2.0.0)**:
  * Removido `%run ./ingest_vendas_base`
  * Leitura direta da tabela Delta implementada
  * Validação da tabela Delta adicionada
  * Temp views ao invés de views persistidas
  * Header do notebook atualizado
  * Sumário final atualizado

* **Arquitetura**:
  * Execução independente (não depende de outro notebook)
  * Lê da fonte de verdade (tabela Delta persistida)
  * Sem duplicação de dados em memória
  * Alinhado com v3.1.0 do ingest_vendas_base

* **Implementação**:
  * Todas as 4 temp views SQL criadas conforme especificação
  * LogControl implementado corretamente com logger_name="vendas_semantic_layer"
  * Validações de qualidade implementadas em todos os blocos
  * Try-except com error_handler() em todas as operações críticas
  * Código estruturado em seções markdown claras
  * Validação da tabela base implementada
  * Sumário final com lista de views criadas

* **Documentação SDD**:
  * plan.md atualizado com arquitetura v2.0.0
  * spec.md atualizado com código completo v2.0.0
  * tasks.md atualizado com estado real pós-refatoração

### Pendente 🟡
* **Testes automatizados**: Precisam ser criados em `src/tests/nb_test_semantic_views`
  * Teste de leitura da tabela Delta
  * Teste de criação das temp views
  * Teste de consultabilidade e contagem de registros
  * Teste de LogControl

### Desvios do Plan/Spec
* **Nenhum** - implementação está 100% conforme especificação v2.0.0

---

**Última Atualização**: 2026-04-19 - Refatoração v2.0.0 concluída, tasks marcadas conforme implementação real, documentação SDD atualizada