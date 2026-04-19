# SDD Instructions - Spec-driven development

## 🎯 Objetivo Central do SDD

O **SDD (Spec-driven development)** é uma metodologia que garante:

1. **📋 Planejamento Completo** - Pensar antes de codificar
2. **📐 Especificação Clara** - Definir exatamente o que construir
3. **✅ Execução Rastreável** - Acompanhar progresso com tasks granulares
4. **🧪 Qualidade Garantida** - Testes automatizados obrigatórios
5. **📊 Rastreabilidade Total** - Logging padronizado e auditável

**Princípio Fundamental**: *"Nunca comece a codificar sem ter SPEC → PLAN → TASKS definidos"*

---

## 📂 Convenção de Paths deste Documento

**Todos os paths relativos são a partir da raiz do projeto: `vendas_regionais/`**

Exemplos:
* `src/nb_vendas_base_ingestion.py` → Arquivo na raiz do projeto
* `sdd/features/vendas_base_ingestion/plan.md` → Documentação SDD
* `tests/test_vendas_base_ingestion.md` → Testes
* `dashboards/dashboard_vendas_regionais.json` → Definição de dashboard exportada

**Exceção**: LogControl centralizado usa path absoluto (está fora do projeto):
* `/Workspace/Users/data.in.code/data-in-code/error_handler_logging/src/logger_control`

---

## 📚 Índice Rápido

1. [Quando Criar uma Feature](#quando-criar-uma-feature)
2. [Fluxo de Trabalho SDD](#fluxo-de-trabalho-sdd)
3. [Estrutura de Arquivos](#estrutura-de-arquivos-obrigatória)
4. [Documentos Obrigatórios](#documentos-obrigatórios-detalhados)
5. [Padrões de Implementação](#padrões-de-implementação)
6. [Logging e Error Handling](#logging-e-error-handling-padronizados)
7. [Anti-Patterns PySpark](#anti-patterns-evite-em-pyspark)
8. [Validação e Checklist](#validação-obrigatória-checklist)
9. [Sistema de Rastreabilidade](#sistema-de-rastreabilidade-traceability)
10. [Dashboards e Visualizações](#dashboards-e-visualizações) ⭐ _NOVO_
11. [Troubleshooting](#troubleshooting-problemas-comuns)

---

[... MANTER TODO O CONTEÚDO EXISTENTE ATÉ "Dúvidas Frequentes" ...]

---

## 📊 Dashboards e Visualizações

### Dashboard Vendas Regionais

**Status**: ✅ Implementado e funcional  
**Versão**: 1.0.0  
**Data de Criação**: 2026-04-19  
**Tipo**: Lakeview Dashboard (Databricks)  

#### Propósito

Dashboard analítico interativo que replica funcionalidades do Excel `VendasRegionaisVBA.xlsm`, permitindo análise visual de vendas com filtros cruzados globais.

#### Identificação do Dashboard

* **Nome**: Dashboard Vendas Regionais
* **ID Databricks**: `01f13c0786871fd189adb02b7e04f008`
* **TreeNode ID**: `4064538371365942`
* **Path Databricks**: `/Users/data.in.code/Dashboard Vendas Regionais.lvdash.json`
* **Definição Versionada**: `dashboards/dashboard_vendas_regionais.json` ✅

#### Arquitetura de Dados

**Fonte de Dados**:
* **Tabela Delta**: `workspace.vendas_regionais.vendas_base`
* **Registros**: 1.000 transações
* **Valor Total**: R$ 7.004.651,22
* **Período**: Jan-Mai 2026

**Dataset Único** (para Filtros Cruzados):
* **Nome**: `datasets/vendas_base_completa`
* **SQL**:
```sql
SELECT 
  data_venda, mes_abrev, regiao, vendedor, 
  codigo_vendedor, secao, valor_vendas, ano, mes
FROM workspace.vendas_regionais.vendas_base
ORDER BY ano, mes  -- ⚠️ CRÍTICO para ordenação cronológica
```
* **Observação Crítica**: `ORDER BY ano, mes` é OBRIGATÓRIO. Widget de linha temporal depende dessa ordenação no SQL, não aceita sort customizado no frontend.

**Colunas Calculadas**:
1. **Trimestre**: `CASE WHEN mes IN (1,2,3) THEN 'Q1' WHEN mes IN (4,5,6) THEN 'Q2'...`
2. **Ordem Mês**: CASE para ordenação numérica (não utilizada)
3. **Mês Ordenado**: CONCAT com prefixo (não utilizada)

#### Filtros Globais (Page: Global Filters)

| # | Nome | Tipo | Coluna | Valores | Posição |
|---|------|------|--------|---------|----------|
| 1 | Data da Venda | Date Range | `data_venda` | - | (0,0) 3x4 |
| 2 | Região | Multi-select | `regiao` | 4 regiões | (3,0) 3x4 |
| 3 | Mês | Multi-select | `mes_abrev` | 5 meses | (6,0) 3x4 |
| 4 | Vendedor | Multi-select | `vendedor` | 8 vendedores | (9,0) 3x4 |
| 5 | Trimestre | Single-select | `Trimestre` (calc) | Q1, Q2 | (4,0) 3x4 |

**IMPORTANTE**: Todos os filtros usam o mesmo dataset `vendas_base_completa` para garantir filtros cruzados funcionais.

#### Visualizações (Page 1 - Layout 2x2)

**1. Desempenho do Vendedor** (vendas_por_vendedor)
* **Tipo**: Bar chart vertical
* **Posição**: (0,0) - 6x7
* **Eixo X**: `vendedor` (ordenado por valor DESC)
* **Eixo Y**: `SUM(valor_vendas)` em BRL
* **Dataset**: vendas_base_completa

**2. Vendas por Região** (vendas_por_regiao)
* **Tipo**: Bar chart vertical
* **Posição**: (6,0) - 6x7
* **Eixo X**: `regiao` (ordenado por valor DESC)
* **Eixo Y**: `SUM(valor_vendas)` em BRL
* **Dataset**: vendas_base_completa

**3. Vendas por Mês** (vendas_por_mes) ⚠️
* **Tipo**: Line chart
* **Posição**: (0,7) - 6x7
* **Eixo X**: `mes_abrev` (JAN→FEV→MAR→ABR→MAI)
* **Eixo Y**: `SUM(valor_vendas)` em BRL
* **Dataset**: vendas_base_completa
* **Ordenação**: Garantida pelo `ORDER BY ano, mes` no SQL do dataset
* **CRÍTICO**: Não usar sort no widget! Ordenação DEVE vir do SQL.

**4. Desempenho da Categoria** (vendas_por_secao)
* **Tipo**: Bar chart horizontal
* **Posição**: (6,7) - 6x7
* **Eixo X**: `SUM(valor_vendas)` em BRL
* **Eixo Y**: `secao` (ordenado por valor DESC)
* **Dataset**: vendas_base_completa
* **Nota**: Excel usa treemap, Databricks não suporta - usamos barras horizontais

#### Tema Visual

* **Paleta de Cores**: Azul profissional (#5B9BD5, #70AD47, #FFC000, #ED7D31...)
* **Canvas Light**: #E8EEF7
* **Canvas Dark**: #1A2332
* **Widgets Light**: #FFFFFF
* **Widgets Dark**: #2D3E50
* **Fonte**: Arial

#### Baseline de Dados (Sem Filtros)

| Métrica | Valor |
|---------|-------|
| Total Geral | R$ 7.004.651,22 |
| Transações | 1.000 |
| Vendedores | 8 |
| Regiões | 4 |
| Meses | 5 |
| Seções | 8 |

**Detalhes**:
* **Vendedores**: Ricardo, Raquel, Renata, Ronaldo, Roberta, Rafael, Rodrigo, Roberto
* **Regiões**: Sul, Nordeste, Sudeste, Norte
* **Meses**: JAN, FEV, MAR, ABR, MAI
* **Seções**: Eletrônicos, Telefonia, Games, Móveis, Informática, Livros, Eletrodomésticos, Automotivo

#### Testes de Filtros Realizados

| Filtro | Resultado | Transações |
|--------|-----------|------------|
| Região = Sul | R$ 1.906.583,44 | 250 |
| Mês = MAR | R$ 1.573.786,55 | 226 |
| Vendedor = Ricardo | R$ 966.266,75 | 131 |
| Sul + MAR (combinado) | R$ 448.572,29 | 61 |

#### Problemas Resolvidos (Lições Aprendidas)

**1. Ordenação de Meses no Gráfico de Linha** 🔴 CRÍTICO

**Sintoma**: Meses aparecem desordenados (ABR, FEV, JAN, MAI, MAR)

**Tentativas Falhadas**:
* ❌ Sort customizado no widget (não respeitado pelo frontend)
* ❌ Coluna calculada "Ordem Mês" no eixo X (mostra números 1,2,3...)
* ❌ Coluna calculada "Mês Ordenado" com prefixo "01_JAN" (não ordena alfanumericamente)

**Solução Final** ✅:
```sql
-- Adicionar ORDER BY no SQL do dataset base
SELECT ... FROM vendas_base
ORDER BY ano, mes  -- ← Ordena ANTES de enviar para o widget
```

**Lição**: Para gráficos de linha temporal, ordenação DEVE ser feita no SQL do dataset, não no widget. Widget respeita ordem de chegada dos dados.

**2. Filtros Cruzados Não Funcionando**

**Causa**: Widgets usando datasets agregados separados (ex: `vendas_por_vendedor`, `vendas_por_regiao`)

**Solução** ✅:
* Deletar todos os datasets agregados
* Criar um único dataset `vendas_base_completa` com dados brutos
* Todos os widgets usam o mesmo dataset
* Agregações (SUM, COUNT) são feitas nos widgets, NÃO no SQL

**Lição**: Para filtros cruzados funcionarem, TODOS os widgets devem compartilhar o mesmo dataset base.

**3. Categorias Desordenadas em Bar Charts**

**Solução** ✅: Adicionar `sort: {by: "value", order: "descending"}` no eixo Y do widget

#### Instruções para Recriação

**Ordem de Execução**:
1. Criar novo dashboard Lakeview
2. Criar dataset `vendas_base_completa` com SQL incluindo `ORDER BY ano, mes`
3. Adicionar colunas calculadas (Trimestre, Ordem Mês, Mês Ordenado)
4. Criar página "Global Filters" com 5 filtros
5. Criar página "Page 1" com 4 widgets em layout 2x2
6. Aplicar tema visual (paleta azul profissional)
7. Testar filtros cruzados interativamente

**Regras de Ouro**:
* ⚠️ `ORDER BY` no dataset é OBRIGATÓRIO para ordenação cronológica
* ⚠️ Todos os widgets devem usar o MESMO dataset para filtros cruzados
* ⚠️ Agregações (SUM, COUNT) são feitas nos WIDGETS, não no SQL
* ⚠️ Coluna calculada "Trimestre" permite filtro trimestral adicional

#### Arquivos Relacionados

* **Excel Referência**: `arquivos/VendasRegionaisVBA.xlsm`
* **Notebook Semantic Views**: `src/nb_create_semantic_views` (ID: 3065207269910782)
* **Definição JSON Exportada**: `dashboards/dashboard_vendas_regionais.json` ✅
* **Este documento**: `sdd_instructions.md`

#### Como Versionar Novos Dashboards

**Quando criar dashboard novo**:
1. Criar dashboard via UI Databricks
2. Exportar definição: Buscar dashboard com `searchAssets`, ler com `readAssetById`
3. Criar arquivo JSON: `dashboards/dashboard_<nome>.json`
4. Documentar nesta seção do SDD
5. Incluir ID, path, datasets, filtros, widgets
6. Documentar problemas resolvidos e lições aprendidas

**Benefícios do versionamento**:
* ✅ Rastreabilidade de mudanças no dashboard
* ✅ Recriação rápida em outros ambientes
* ✅ Documentação técnica para manutenção
* ✅ Histórico de decisões de design

#### 🔧 Placeholders e Substituições

**⚠️ REGRA OBRIGATÓRIA**: Ao utilizar arquivos JSON de dashboards deste projeto, substituir placeholders pelos valores reais do ambiente.

**Placeholder `<USER_EMAIL>`**:

Quando encontrado em paths dentro de arquivos JSON (ex: `dashboards/dashboard_vendas_regionais.json`):

```json
{
  "path_databricks": "/Users/<USER_EMAIL>/Dashboard Vendas Regionais.lvdash.json"
}
```

**Ação Requerida**:
* Substituir `<USER_EMAIL>` pelo **email real do usuário corrente do Databricks**
* Exemplo: `/Users/data.in.code@gmail.com/Dashboard Vendas Regionais.lvdash.json`

**Razão do Placeholder**:
* ✅ **Versionamento Seguro**: Evita commit de dados pessoais (PII) no Git
* ✅ **Portabilidade**: Arquivo JSON funciona para múltiplos usuários/ambientes
* ✅ **Governança**: Conformidade com políticas de segurança (veja `.gitignore`)

**Workflow Correto**:

1. **Ler arquivo JSON** com placeholder: `dashboards/dashboard_vendas_regionais.json`
2. **Identificar usuário corrente**: Obter email do workspace atual
3. **Substituir placeholder**: Trocar `<USER_EMAIL>` pelo email real
4. **Usar configuração**: Aplicar JSON com paths corretos no Databricks

**Exemplo Prático**:

```python
# ERRADO - Usar path com placeholder
path = "/Users/<USER_EMAIL>/dashboard.lvdash.json"

# CORRETO - Substituir por email real
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
path = f"/Users/{user_email}/dashboard.lvdash.json"
```

**Outros Placeholders**:
* Se novos placeholders forem criados no futuro, documentá-los aqui
* Padrão de nomenclatura: `<NOME_VARIAVEL>` em MAIÚSCULAS

---

## 📞 Suporte

**Referências**:
* Feature de exemplo: `sdd/features/vendas_base_ingestion/`
* LogControl centralizado: `/data-in-code/error_handler_logging/`
* Dashboard versionado: `dashboards/dashboard_vendas_regionais.json`
* Este documento: `sdd_instructions.md`

**Estrutura de Diretórios (Relative Paths)**:
* Código: `src/`
* Testes: `tests/`
* Documentação: `sdd/features/`
* Dados de entrada: `arquivos/`
* Dashboards: `dashboards/` ⭐ _NOVO_

**Governança**: Esta instrução é obrigatória para todo desenvolvimento no projeto vendas_regionais.

---

*Versão 3.3 - Adicionada seção de Placeholders e Substituições (2026-04-19)*