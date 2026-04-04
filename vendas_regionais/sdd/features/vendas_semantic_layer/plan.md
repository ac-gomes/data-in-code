# Plan: Vendas Semantic Layer

## Propósito

Criar uma camada semântica de views SQL que agregam os dados da tabela base de vendas (`main.vendas_regionais.tb_vendas_base`) para facilitar análises e dashboards. As views replicam as agregações pré-calculadas encontradas na aba "Base Grafico" do arquivo Excel original.

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

### Abordagem Técnica

1. **SQL Puro**: Views serão criadas usando SQL puro (CREATE OR REPLACE VIEW)
2. **Nomenclatura**: Prefixo `vw_` para indicar que são views (não tabelas materializadas)
3. **Schema**: Todas as views no schema `main.vendas_regionais`
4. **Fonte de Dados**: Tabela `main.vendas_regionais.tb_vendas_base`

### Benefícios

* **Performance**: Views são leves e não duplicam dados
* **Consistência**: Sempre refletem dados atualizados da tabela base
* **Simplicidade**: Lógica SQL clara e manutenível
* **Reusabilidade**: Views podem ser consumidas por múltiplos dashboards e queries

### Justificativa para Views (não Tabelas Materializadas)

* **Volume Baixo**: ~90 registros na base não justificam materialização
* **Agregações Simples**: GROUP BY simples são extremamente rápidos
* **Atualização Frequente**: Views sempre refletem dados mais recentes
* **Economia de Armazenamento**: Não duplicam dados

## Logging e Tratamento de Erros

### Padrão LogControl

* **Obrigatório**: Uso da classe `LogControl` para todos os logs de criação das views
* **Rastreabilidade**: Captura de exceções durante criação das views
* **Níveis de Log**:
  * INFO: Início da criação de cada view
  * SUCCESS: View criada com sucesso
  * WARNING: Divergências entre agregações e Excel
  * ERROR: Falhas na criação das views

### Validação Contra Excel

Após criar as views, validar que os totais correspondem aos valores da aba "Base Grafico":

* Ler aba "Base Grafico" do Excel
* Comparar totais calculados vs. totais nas views
* Logar warnings se houver divergências > 0.01 (tolerância para arredondamento)

## Dependências

* **Tabela Base**: `main.vendas_regionais.tb_vendas_base` (criada pela feature vendas_base_ingestion)
* **Feature**: error_handler_logging (LogControl)
* **Arquivo de Referência**: VendasRegionaisVBA.xlsm (aba "Base Grafico" para validação)

## Métricas de Sucesso

1. ✅ Todas as 4 views criadas sem erros
2. ✅ Totais nas views correspondem à aba "Base Grafico" (margem de erro < 0.01%)
3. ✅ Todas as views são consultáveis via SQL
4. ✅ Logs de criação persistidos via LogControl
5. ✅ Testes automatizados passam 100%

## Próximos Passos

1. Criar especificação técnica detalhada (`spec.md`) com SQL completo
2. Definir tarefas granulares de implementação (`tasks.md`)
3. Implementar SQL de criação das views
4. Validar agregações contra Excel
5. Criar testes automatizados
