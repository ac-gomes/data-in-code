# Plan: Dashboard Vendas Regionais (DVR)

**Feature Code**: DVR  
**Versão**: 1.0.0  
**Data**: 2026-04-19  
**Status**: ✅ Implementado

---

## 1. Propósito

Criar um dashboard interativo Databricks Lakeview que replique as funcionalidades analíticas do Excel `VendasRegionaisVBA.xlsm`, permitindo análise visual de vendas com filtros cruzados globais.

O dashboard fornece visão consolidada de vendas por:
- **Vendedor** (desempenho individual)
- **Região** (distribuição geográfica)
- **Mês** (evolução temporal)
- **Categoria/Seção** (performance por tipo de produto)

### Objetivo Principal

**Transformar análise estática do Excel em dashboard interativo e self-service**, permitindo que usuários de negócio explorem dados de vendas sem conhecimento técnico, usando filtros intuitivos.

---

## 2. Contexto de Negócio

### Problema Resolvido

**Antes (Excel)**:
- ❌ Análise limitada a planilhas estáticas
- ❌ Falta de interatividade (filtros manuais via VBA)
- ❌ Dificuldade de atualização (dependência de macros)
- ❌ Compartilhamento limitado (arquivo local)
- ❌ Sem governança de dados

**Depois (Dashboard Lakeview)**:
- ✅ Análise interativa com filtros cruzados
- ✅ Atualização automática (conectado à tabela Delta)
- ✅ Acesso centralizado (Databricks Workspace)
- ✅ Governança via Unity Catalog
- ✅ Escalabilidade para grandes volumes

### Fonte de Dados

**Tabela Delta**: `workspace.vendas_regionais.vendas_base`
- **Pipeline de Ingestão**: `nb_vendas_base_ingestion` (feature VBI)
- **Geração Sintética**: `nb_synthetic_data_generator`
- **Camada Semântica**: `nb_create_semantic_views` (feature VSL)

**Período de Dados**: Jan-Mai 2026  
**Volume**: 1.000 transações  
**Valor Total**: R$ 7.004.651,22

---

## 3. Regras de Negócio

### Regra 1: Dataset Único para Filtros Cruzados

**Descrição**: Todos os widgets devem compartilhar o mesmo dataset base para garantir que filtros globais funcionem de forma cruzada.

**Exemplo**:
- Usuário filtra **Região = Sul**
- TODOS os 4 widgets (vendedor, região, mês, seção) devem refletir apenas dados do Sul
- Se widgets usassem datasets agregados separados, filtros não funcionariam

**Exceção**: Nenhuma. Esta regra é absoluta.

**Implementação Técnica**:
- ✅ Criar dataset `vendas_base_completa` com dados brutos
- ✅ Agregar (SUM, COUNT) nos widgets, NÃO no SQL do dataset
- ❌ NUNCA criar datasets agregados separados (ex: `vendas_por_vendedor_dataset`)

---

### Regra 2: Ordenação Cronológica Garantida pelo SQL

**Descrição**: Gráficos de linha temporal (vendas por mês) DEVEM manter ordem cronológica (JAN→FEV→MAR→ABR→MAI).

**Problema Identificado**: 
Widgets Databricks Lakeview NÃO respeitam configuração de sort customizado no frontend. Tentativas de ordenar no widget resultam em ordem alfabética (ABR, FEV, JAN, MAI, MAR).

**Solução Obrigatória**:
```sql
-- Dataset DEVE incluir ORDER BY
SELECT * FROM vendas_base
ORDER BY ano, mes  -- ⚠️ CRÍTICO!
```

**Exemplo de Falha**:
- ❌ Tentar ordenar via `sort: {by: "mes_abrev"}` no widget → NÃO funciona
- ❌ Usar coluna calculada "Ordem Mês" no eixo X → Mostra números (1,2,3)
- ❌ Usar coluna calculada "Mês Ordenado" (01_JAN) → NÃO ordena

**Única Solução Funcional**:
- ✅ `ORDER BY ano, mes` no SQL do dataset
- Widget respeita ordem de chegada dos dados

**Exceção**: Nenhuma. Qualquer tentativa de ordenar no frontend falhará.

---

### Regra 3: Filtros Globais Devem Cobrir Todas as Dimensões

**Descrição**: Dashboard deve permitir análise por todas as dimensões de negócio relevantes.

**Filtros Obrigatórios**:
1. **Data da Venda** (date range) - Período temporal
2. **Região** (multi-select) - Distribuição geográfica
3. **Mês** (multi-select) - Sazonalidade
4. **Vendedor** (multi-select) - Performance individual
5. **Trimestre** (single-select) - Análise trimestral

**Exemplo de Uso**:
- Gestor quer ver desempenho da **Região Sul** no **Q1** (Jan-Mar)
- Aplica filtros: Região=Sul, Trimestre=Q1
- Dashboard mostra apenas vendedores, meses e categorias do Sul no Q1

**Exceção**: Filtro de categoria/seção foi intencionalmente omitido (8 valores - seria poluição visual).

---

### Regra 4: Baseline de Dados Conhecido

**Descrição**: Dashboard deve ter baseline documentado para validação de cálculos.

**Valores Esperados (SEM FILTROS)**:
- Total Geral: R$ 7.004.651,22
- Transações: 1.000
- Vendedores: 8
- Regiões: 4
- Meses: 5
- Seções: 8

**Exemplo de Validação**:
- Soma de TODOS os widgets deve resultar em R$ 7.004.651,22
- Se divergir, há erro de configuração (ex: dataset errado, filtro oculto)

**Exceção**: Valores mudam se dados forem regerados pelo synthetic data generator.

---

### Regra 5: Valores Monetários em Formato BRL

**Descrição**: Todos os valores de vendas devem usar formato de moeda brasileira.

**Formato Esperado**:
- Símbolo: R$
- Separador de milhar: ponto (.)
- Separador decimal: vírgula (,)
- Exemplo: R$ 1.906.583,44

**Implementação**: Configurar `numberFormat: "currency"` e `currencyCode: "BRL"` nos widgets.

**Exceção**: Nenhuma. Valores sem formato correto causam confusão.

---

## 4. Estratégia de Implementação

### Abordagem Técnica

**Tecnologia**: Databricks Lakeview Dashboard  
**Justificativa**:
- ✅ Nativo Databricks (sem ferramentas externas)
- ✅ Conectado diretamente a Unity Catalog
- ✅ Filtros cruzados suportados nativamente
- ✅ Visualizações interativas sem código

**Alternativas Consideradas e Rejeitadas**:
- ❌ **Plotly/Matplotlib em notebook**: Não é self-service, requer execução manual
- ❌ **Tableau/Power BI**: Ferramentas externas, custos adicionais
- ❌ **SQL Dashboards (legado)**: Sendo descontinuado pelo Databricks

### Arquitetura do Dashboard

```
Tabela Delta (workspace.vendas_regionais.vendas_base)
           ↓
Dataset Único: vendas_base_completa (SQL com ORDER BY)
           ↓
┌──────────────────────────────────────┐
│  Global Filters (Page 1)             │
│  - Data, Região, Mês, Vendedor, Q    │
└──────────────────────────────────────┘
           ↓ (filtros aplicados)
┌──────────────────────────────────────┐
│  Visualizations (Page 2)             │
│  Layout 2x2:                         │
│  ┌─────────┬─────────┐               │
│  │Vendedor │ Região  │               │
│  ├─────────┼─────────┤               │
│  │  Mês    │ Seção   │               │
│  └─────────┴─────────┘               │
└──────────────────────────────────────┘
```

### Fases de Implementação

**Fase 1: Preparação de Dados**
1. Criar dataset base com ORDER BY (crítico para ordenação)
2. Adicionar colunas calculadas (Trimestre, Ordem Mês)
3. Validar baseline de dados

**Fase 2: Filtros Globais**
1. Criar página "Global Filters"
2. Adicionar 5 filtros usando mesmo dataset
3. Testar filtros cruzados

**Fase 3: Visualizações**
1. Criar página principal (Page 1)
2. Adicionar 4 widgets em layout 2x2
3. Configurar agregações (SUM, COUNT) nos widgets
4. Aplicar formatação de moeda BRL

**Fase 4: Tema e Validação**
1. Aplicar tema visual profissional (paleta azul)
2. Validar baseline de dados
3. Testar filtros combinados
4. Documentar problemas resolvidos

---

## 5. Dependências

### Features Upstream (Obrigatórias)

| Feature | Status | Razão |
|---------|--------|-------|
| **synthetic_data_generator** | ✅ | Gera dados sintéticos de vendas |
| **vendas_base_ingestion** (VBI) | ✅ | Persiste dados na tabela Delta |
| **vendas_semantic_layer** (VSL) | 🟡 | Opcional - dashboard acessa tabela direta |

**Nota**: VSL cria temp views SQL, mas dashboard usa tabela Delta diretamente (não depende das views).

### Tabelas Requeridas

- **workspace.vendas_regionais.vendas_base** (Delta Table)
  - Colunas: data_venda, mes_abrev, regiao, vendedor, codigo_vendedor, secao, valor_vendas, ano, mes
  - Volume: ~1.000 registros
  - Período: Jan-Mai 2026

### Permissões

- **SELECT** em `workspace.vendas_regionais.vendas_base`
- **CREATE DASHBOARD** no Databricks Workspace
- **USE CATALOG** workspace
- **USE SCHEMA** vendas_regionais

---

## 6. Métricas de Sucesso

### Critérios de Aceitação

**Funcional**:
- [ ] Dashboard renderiza sem erros
- [ ] 5 filtros globais funcionam de forma cruzada
- [ ] 4 visualizações exibem dados corretos
- [ ] Ordenação cronológica (JAN→MAI) está correta
- [ ] Valores monetários em formato BRL
- [ ] Baseline de dados valida (R$ 7.004.651,22)

**Não-Funcional**:
- [ ] Dashboard carrega em < 5 segundos
- [ ] Filtros respondem em < 2 segundos
- [ ] Layout responsivo (funciona em diferentes resoluções)
- [ ] Tema visual profissional aplicado

**Documentação**:
- [ ] Definição JSON exportada (`dashboards/dashboard_vendas_regionais.json`)
- [ ] Documentação SDD completa (plan, spec, tasks, traceability)
- [ ] Problemas resolvidos documentados (lições aprendidas)
- [ ] README.md atualizado
- [ ] sdd_instructions.md atualizado

### Validação de Filtros

**Testes Realizados com Sucesso**:

| Filtro Aplicado | Resultado Esperado | Status |
|-----------------|-------------------|--------|
| Região = Sul | R$ 1.906.583,44 (250 tx) | ✅ |
| Mês = MAR | R$ 1.573.786,55 (226 tx) | ✅ |
| Vendedor = Ricardo | R$ 966.266,75 (131 tx) | ✅ |
| Sul + MAR (combinado) | R$ 448.572,29 (61 tx) | ✅ |
| Trimestre = Q1 | R$ 4.206.477,86 (558 tx) | ✅ |

---

## 7. Riscos e Mitigações

### Risco 1: Ordenação Cronológica Quebrada

**Probabilidade**: Alta (já ocorreu)  
**Impacto**: Crítico (gráfico de linha inutilizado)

**Mitigação**:
- ✅ Sempre usar `ORDER BY ano, mes` no dataset
- ✅ Documentar que ordenação no widget NÃO funciona
- ✅ Testar ordenação após QUALQUER mudança no dataset

### Risco 2: Filtros Cruzados Param de Funcionar

**Probabilidade**: Média (se criar datasets separados)  
**Impacto**: Crítico (principal funcionalidade)

**Mitigação**:
- ✅ NUNCA criar datasets agregados separados
- ✅ Validar que todos os widgets usam `vendas_base_completa`
- ✅ Testar filtros combinados regularmente

### Risco 3: Baseline de Dados Diverge

**Probabilidade**: Baixa  
**Impacto**: Médio (confiança nos dados)

**Mitigação**:
- ✅ Documentar baseline esperado
- ✅ Validar soma total após mudanças
- ✅ Alertar se synthetic data generator for re-executado

---

## 8. Próximos Passos

### Melhorias Futuras (Backlog)

1. **Drill-down por Vendedor**: Click em barra → detalhe de transações
2. **Comparação Temporal**: YoY, MoM, QoQ
3. **Alertas**: Vendas abaixo de meta, anomalias
4. **Exportação**: Excel, PDF via API
5. **Métricas Calculadas**: Ticket médio, taxa de conversão
6. **Mapa Geográfico**: Heatmap de regiões (se houver lat/long)
7. **Forecasting**: Previsão de vendas com AI functions

### Manutenção Contínua

- **Semanal**: Validar baseline de dados
- **Mensal**: Revisar filtros e adicionar novos se necessário
- **Trimestral**: Avaliar performance e otimizar queries
- **Anual**: Revisar tema visual e UX

---

## 9. Referências

- **Excel Original**: `arquivos/VendasRegionaisVBA.xlsm`
- **Tabela Delta**: `workspace.vendas_regionais.vendas_base`
- **Notebook Semantic Layer**: `src/nb_create_semantic_views`
- **Definição JSON**: `dashboards/dashboard_vendas_regionais.json`
- **SDD Instructions**: `sdd_instructions.md` (seção "Dashboards e Visualizações")
- **README**: `README.md` (seção "Dashboard Interativo")

---

**Última Atualização**: 2026-04-19  
**Status**: ✅ Implementado e Validado  
**Próxima Revisão**: 2026-07-19 (trimestral)