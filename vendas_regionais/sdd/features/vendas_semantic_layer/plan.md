# Plan: Vendas Semantic Layer

**Versão**: 2.0.0 (atualizada para v3.1.0 do ingest_vendas_base)  
**Última Atualização**: 2026-04-19

## Propósito

Criar uma camada semântica de **temp views SQL** que agregam os dados da **tabela Delta** `workspace.vendas_regionais.vendas_base` para facilitar análises e dashboards. As views replicam as agregações pré-calculadas encontradas na aba "Base Grafico" do arquivo Excel original.

## Contexto de Negócio

A aba "Base Grafico" do arquivo `VendasRegionaisVBA.xlsm` contém 4 agregações pré-calculadas que servem como modelo semântico para dashboards analíticos:

1. **Vendas por Vendedor**: Total de vendas agrupado por nome do vendedor
2. **Vendas por Região**: Total de vendas agrupado por região geográfica
3. **Vendas por Mês**: Total de vendas agrupado por mês
4. **Vendas por Seção**: Total de vendas agrupado por seção/categoria de produto

Essas agregações eliminam a necessidade de recalcular totais repetidamente e garantem consistência nas análises.

## Regras de Negócio

### Views a Serem Criadas

#### 1. vw_vendas_por_vendedor
* **Propósito**: Agregar vendas totais por vendedor
* **Agrupamento**: `vendedor`
* **Métricas**: SUM(valor_vendas) AS total_vendas
* **Ordenação**: Decrescente por total_vendas

#### 2. vw_vendas_por_regiao
* **Propósito**: Agregar vendas totais por região geográfica
* **Agrupamento**: `regiao`
* **Métricas**: SUM(valor_vendas) AS total_vendas
* **Ordenação**: Decrescente por total_vendas

#### 3. vw_vendas_por_mes
* **Propósito**: Agregar vendas totais por mês
* **Agrupamento**: `mes`
* **Métricas**: SUM(valor_vendas) AS total_vendas
* **Ordenação**: Cronológica (JAN, FEV, MAR, ABR, MAI)

#### 4. vw_vendas_por_secao
* **Propósito**: Agregar vendas totais por seção/categoria
* **Agrupamento**: `secao`
* **Métricas**: SUM(valor_vendas) AS total_vendas
* **Ordenação**: Decrescente por total_vendas

### Validações de Qualidade

* Total geral de vendas em todas as views deve ser igual ao total na tabela base
* Valores das agregações devem corresponder aos valores na aba "Base Grafico" do Excel
* Todas as views devem ser consultáveis sem erros
* Nenhuma view deve retornar valores nulos

## Estratégia de Implementação

### Abordagem Técnica - v2.0.0

**MUDANÇA ARQUITETURAL (v3.1.0 do ingest_vendas_base):**

1. **Fonte de Dados**: Tabela Delta `workspace.vendas_regionais.vendas_base` (Unity Catalog)
2. **Leitura Direta**: `spark.table("workspace.vendas_regionais.vendas_base")` (não mais via %run)
3. **Temp Views**: Criadas com `createOrReplaceTempView()` (disponíveis apenas na sessão Spark)
4. **Nomenclatura**: Prefixo `vw_` para indicar que são views agregadas
5. **Independência**: Notebook pode ser executado de forma independente (não precisa executar outro notebook primeiro)

### Fluxo de Execução

```
1. Ler tabela Delta (workspace.vendas_regionais.vendas_base)
2. Validar que a tabela existe e tem dados
3. Criar temp view base (vendas_base_temp)
4. Criar 4 temp views agregadas usando SQL
5. Exibir resultados e estatísticas
6. Validações de qualidade
```

### Benefícios da Nova Arquitetura

* **Independência**: Não precisa executar outro notebook via %run
* **Performance**: Lê da fonte de verdade (tabela Delta persistida)
* **Simplicidade**: Lógica SQL clara e manutenível
* **Reusabilidade**: Views podem ser consultadas durante toda a sessão
* **Sem Duplicação**: Não mantém dados em memória duplicados

### Justificativa para Temp Views (não Views Persistidas)

* **Volume Baixo**: ~1000 registros na base não justificam persistência
* **Agregações Simples**: GROUP BY simples são extremamente rápidos
* **Exploração**: Temp views são ideais para análise interativa
* **Economia de Armazenamento**: Não duplicam dados no Unity Catalog
* **Flexibilidade**: Podem ser recriadas/modificadas facilmente durante análise

## Logging e Tratamento de Erros

### Padrão LogControl

* **Obrigatório**: Uso da classe `LogControl` para todos os logs de criação das views
* **Tabela de Logs**: `main.vendas_regionais.tb_logs_semantic`
* **Rastreabilidade**: Captura de exceções durante criação das views
* **Níveis de Log**:
  * INFO: Início da criação de cada view
  * SUCCESS: View criada com sucesso
  * WARNING: Divergências entre agregações e Excel
  * ERROR: Falhas na criação das views

### Validação da Tabela Base

* Verificar que `workspace.vendas_regionais.vendas_base` existe
* Validar que a tabela tem dados (count > 0)
* Exibir schema e estatísticas básicas
* Logar erro se a tabela estiver vazia

## Dependências

* **Tabela Base**: `workspace.vendas_regionais.vendas_base` (criada pela feature vendas_base_ingestion v3.1.0)
* **Feature**: error_handler_logging (LogControl)
* **Unity Catalog**: workspace (catálogo), vendas_regionais (schema)
* **Compute**: Databricks Serverless

## Métricas de Sucesso

1. ✅ Todas as 4 temp views criadas sem erros
2. ✅ Totais nas views são consistentes internamente
3. ✅ Todas as views são consultáveis via SQL na sessão
4. ✅ Logs de criação persistidos via LogControl
5. ✅ Notebook pode ser executado de forma independente
6. ✅ Validação da tabela base implementada

## Próximos Passos

1. ✅ Especificação técnica detalhada (`spec.md`) atualizada
2. ✅ Tarefas granulares de implementação (`tasks.md`) atualizadas
3. ✅ Implementação alinhada com v3.1.0 do ingest_vendas_base
4. 📋 Validar execução completa do notebook
5. 📋 Atualizar tasks.md com estado real pós-mudanças
