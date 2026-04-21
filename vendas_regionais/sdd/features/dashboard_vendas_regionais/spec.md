# Especificação Técnica: Dashboard Vendas Regionais (DVR)

**Feature Code**: DVR  
**Versão**: 1.0.0  
**Data**: 2026-04-19  
**Tipo**: Databricks Lakeview Dashboard

---

## 1. Visão Geral

Dashboard analítico interativo Databricks Lakeview com 5 filtros globais cruzados e 4 visualizações em layout 2x2, conectado diretamente à tabela Delta `workspace.vendas_regionais.vendas_base`.

**Arquitetura Chave**:
- **1 dataset único** para filtros cruzados
- **2 páginas**: Global Filters + Visualizações
- **5 filtros** globais
- **4 widgets** analíticos
- **ORDER BY no SQL** para ordenação cronológica

---

## 2. Arquitetura de Dados

### 2.1 Input

**Tabela Delta**:
- **Nome**: `workspace.vendas_regionais.vendas_base`
- **Catalog**: workspace (Unity Catalog)
- **Schema**: vendas_regionais
- **Formato**: Delta Lake
- **Localização**: Gerenciada pelo Unity Catalog

**Schema da Tabela**:

| Coluna | Tipo | Descrição | Exemplo |
|--------|------|-------------|----------|
| `data_venda` | DATE | Data da transação | 2026-01-15 |
| `mes_abrev` | STRING | Mês abreviado (3 letras) | "JAN" |
| `regiao` | STRING | Região de vendas | "Sul" |
| `vendedor` | STRING | Nome do vendedor | "Ricardo" |
| `codigo_vendedor` | STRING | Código alfanumérico | "V001" |
| `secao` | STRING | Categoria de produto | "Eletrônicos" |
| `valor_vendas` | DECIMAL(10,2) | Valor da venda em R$ | 7543.21 |
| `ano` | INT | Ano da venda | 2026 |
| `mes` | INT | Mês numérico (1-12) | 1 |
| `data_carga` | TIMESTAMP | Data de ingestão | 2026-04-19 15:30:00 |

**Volume Esperado**: ~1.000 registros  
**Período**: Jan-Mai 2026  
**Valor Total**: R$ 7.004.651,22

**Valores Únicos por Coluna**:
- `regiao`: 4 valores (Sul, Nordeste, Sudeste, Norte)
- `vendedor`: 8 valores (Ricardo, Raquel, Renata, Ronaldo, Roberta, Rafael, Rodrigo, Roberto)
- `mes_abrev`: 5 valores (JAN, FEV, MAR, ABR, MAI)
- `secao`: 8 valores (Eletrônicos, Telefonia, Games, Móveis, Informática, Livros, Eletrodomésticos, Automotivo)

---

### 2.2 Output (Dashboard)

**Tipo**: Databricks Lakeview Dashboard  
**ID Databricks**: `01f13c0786871fd189adb02b7e04f008`  
**TreeNode ID**: `4064538371365942`  
**Path**: `/Users/data.in.code/Dashboard Vendas Regionais.lvdash.json`  
**Definição Versionada**: `dashboards/dashboard_vendas_regionais.json`

**Estrutura**:
```
Dashboard Vendas Regionais
├── datasets/
│   └── vendas_base_completa (SQL query + calculated columns)
├── pages/
│   ├── global_filters/
│   │   ├── filtro_data_venda (date-range)
│   │   ├── filtro_regiao (multi-select)
│   │   ├── filtro_mes (multi-select)
│   │   ├── filtro_vendedor (multi-select)
│   │   └── filtro_trimestre (single-select)
│   └── page1/
│       ├── vendas_por_vendedor (bar chart)
│       ├── vendas_por_regiao (bar chart)
│       ├── vendas_por_mes (line chart)
│       └── vendas_por_secao (bar chart horizontal)
└── theme/
    └── (paleta azul profissional)
```

---

## 3. Fluxo de Processamento

### 3.1 Fase 1: Criação do Dashboard

```python
# Via Databricks UI ou API
# 1. Navegar para Dashboards
# 2. Clicar em "Create Dashboard"
# 3. Escolher "Lakeview Dashboard"
# 4. Nomear: "Dashboard Vendas Regionais"
```

**Resultado**: Dashboard vazio criado

---

### 3.2 Fase 2: Criar Dataset Base

**Nome**: `vendas_base_completa`  
**Display Name**: "Vendas Base Completa"

**SQL Query** (⚠️ ORDER BY é CRÍTICO!):

```sql
SELECT
  data_venda,
  mes_abrev,
  regiao,
  vendedor,
  codigo_vendedor,
  secao,
  valor_vendas,
  ano,
  mes
FROM
  workspace.vendas_regionais.vendas_base
ORDER BY
  ano,
  mes
```

**⚠️ ATENÇÃO**: 
- `ORDER BY ano, mes` é **OBRIGATÓRIO** para ordenação cronológica
- Sem ORDER BY, gráfico de linha mostra meses desordenados (ABR, FEV, JAN, MAI, MAR)
- NÃO tentar ordenar no widget - NÃO funciona no Databricks Lakeview

**Colunas Retornadas**:
- data_venda (DATE)
- mes_abrev (STRING) - JAN, FEV, MAR, ABR, MAI
- regiao (STRING) - Sul, Nordeste, Sudeste, Norte
- vendedor (STRING) - 8 vendedores
- codigo_vendedor (STRING) - V001-V008
- secao (STRING) - 8 categorias
- valor_vendas (DECIMAL)
- ano (INT) - 2026
- mes (INT) - 1, 2, 3, 4, 5

**Registros**: ~1.000 linhas  
**Tempo de Execução**: < 1 segundo

---

### 3.3 Fase 3: Adicionar Colunas Calculadas ao Dataset

No dataset `vendas_base_completa`, adicionar:

#### Coluna Calculada 1: Trimestre

**Nome**: `Trimestre`  
**Display Name**: "Trimestre"  
**Expressão SQL**:

```sql
CASE 
  WHEN `mes` IN (1, 2, 3) THEN 'Q1'
  WHEN `mes` IN (4, 5, 6) THEN 'Q2'
  WHEN `mes` IN (7, 8, 9) THEN 'Q3'
  WHEN `mes` IN (10, 11, 12) THEN 'Q4'
  ELSE 'Desconhecido'
END
```

**Descrição**: "Classificação trimestral baseada no mês (Q1, Q2, Q3, Q4)"

**Valores Esperados**: Q1, Q2 (dados contidos apenas nesses trimestres)

#### Coluna Calculada 2: Ordem Mês (Opcional)

**Nome**: `Ordem Mês`  
**Display Name**: "Ordem Mês"  
**Expressão SQL**:

```sql
CASE `mes_abrev`
  WHEN 'JAN' THEN 1
  WHEN 'FEV' THEN 2
  WHEN 'MAR' THEN 3
  WHEN 'ABR' THEN 4
  WHEN 'MAI' THEN 5
  WHEN 'JUN' THEN 6
  WHEN 'JUL' THEN 7
  WHEN 'AGO' THEN 8
  WHEN 'SET' THEN 9
  WHEN 'OUT' THEN 10
  WHEN 'NOV' THEN 11
  WHEN 'DEZ' THEN 12
  ELSE 99
END
```

**Descrição**: "Ordenação numérica de meses (1-12)"

**⚠️ Nota**: Esta coluna NÃO é usada nos widgets (se usar no eixo X, mostra números ao invés de "JAN", "FEV"). Mantida apenas para referência.

#### Coluna Calculada 3: Mês Ordenado (Opcional)

**Nome**: `Mês Ordenado`  
**Display Name**: "Mês Ordenado"  
**Expressão SQL**:

```sql
CONCAT(LPAD(CAST(`mes` AS STRING), 2, '0'), '_', `mes_abrev`)
```

**Descrição**: "Formato 01_JAN, 02_FEV para ordenação alfanumérica"

**Valores**: 01_JAN, 02_FEV, 03_MAR, 04_ABR, 05_MAI

**⚠️ Nota**: Esta coluna também NÃO funciona para ordenação no widget. Mantida apenas para referência.

---

### 3.4 Fase 4: Criar Página "Global Filters"

**Nome**: `global_filters`  
**Display Name**: "Global Filters"

#### Filtro 1: Data da Venda

**Widget Name**: `filtro_data_venda`  
**Title**: "Data da Venda"  
**Type**: `filter-date-range-picker`  
**Dataset**: `datasets/vendas_base_completa`  
**Column**: `data_venda`  
**Position**: Row 0, Column 0  
**Size**: Width 3, Height 4

**Configuração JSON**:
```json
{
  "type": "filter-date-range-picker",
  "columns": [
    {
      "datasetRefName": "datasets/vendas_base_completa",
      "column": {
        "columnName": "data_venda",
        "columnExpression": "`data_venda`"
      }
    }
  ],
  "name": "Data da Venda"
}
```

#### Filtro 2: Região

**Widget Name**: `filtro_regiao`  
**Title**: "Região"  
**Type**: `filter-multi-select`  
**Dataset**: `datasets/vendas_base_completa`  
**Column**: `regiao`  
**Allow Multiple**: true  
**Position**: Row 0, Column 3  
**Size**: Width 3, Height 4

**Valores Esperados**: Nordeste, Norte, Sudeste, Sul

**Configuração JSON**:
```json
{
  "type": "filter-multi-select",
  "columns": [
    {
      "datasetRefName": "datasets/vendas_base_completa",
      "column": {
        "columnName": "regiao",
        "columnExpression": "`regiao`"
      }
    }
  ],
  "name": "Região",
  "allowMultipleSelection": true
}
```

#### Filtro 3: Mês

**Widget Name**: `filtro_mes`  
**Title**: "Mês"  
**Type**: `filter-multi-select`  
**Dataset**: `datasets/vendas_base_completa`  
**Column**: `mes_abrev`  
**Allow Multiple**: true  
**Position**: Row 0, Column 6  
**Size**: Width 3, Height 4

**Valores Esperados**: JAN, FEV, MAR, ABR, MAI

**Configuração JSON**:
```json
{
  "type": "filter-multi-select",
  "columns": [
    {
      "datasetRefName": "datasets/vendas_base_completa",
      "column": {
        "columnName": "mes_abrev",
        "columnExpression": "`mes_abrev`"
      }
    }
  ],
  "name": "Mês",
  "allowMultipleSelection": true
}
```

#### Filtro 4: Vendedor

**Widget Name**: `filtro_vendedor`  
**Title**: "Vendedor"  
**Type**: `filter-multi-select`  
**Dataset**: `datasets/vendas_base_completa`  
**Column**: `vendedor`  
**Allow Multiple**: true  
**Position**: Row 0, Column 9  
**Size**: Width 3, Height 4

**Valores Esperados**: Ricardo, Raquel, Renata, Ronaldo, Roberta, Rafael, Rodrigo, Roberto

**Configuração JSON**:
```json
{
  "type": "filter-multi-select",
  "columns": [
    {
      "datasetRefName": "datasets/vendas_base_completa",
      "column": {
        "columnName": "vendedor",
        "columnExpression": "`vendedor`"
      }
    }
  ],
  "name": "Vendedor",
  "allowMultipleSelection": true
}
```

#### Filtro 5: Trimestre

**Widget Name**: `filtro_trimestre`  
**Title**: "Trimestre"  
**Type**: `filter-single-select`  
**Dataset**: `datasets/vendas_base_completa`  
**Column (Calculated)**: `Trimestre`  
**Allow Multiple**: true  
**Position**: Row 4, Column 0  
**Size**: Width 3, Height 4

**Valores Esperados**: Q1, Q2

**Configuração JSON**:
```json
{
  "type": "filter-single-select",
  "columns": [
    {
      "datasetRefName": "datasets/vendas_base_completa",
      "column": {
        "columnName": "Trimestre",
        "columnExpression": "`Trimestre`"
      }
    }
  ],
  "name": "Trimestre",
  "allowMultipleSelection": true
}
```

---

### 3.5 Fase 5: Criar Página "Page 1" (Visualizações)

**Nome**: `8f7d2959` (gerado automaticamente)  
**Display Name**: "Page 1"

**Layout**: 2x2 grid (4 widgets em layout simétrico)

#### Widget 1: Desempenho do Vendedor

**Widget Name**: `vendas_por_vendedor`  
**Title**: "Desempenho do Vendedor"  
**Type**: `bar` (vertical bar chart)  
**Dataset**: `datasets/vendas_base_completa`  
**Position**: Row 0, Column 0  
**Size**: Width 6, Height 7

**xAxis**:
- Column: `vendedor`
- Expression: `` `vendedor` ``
- Sort: by value, descending

**yAxis**:
- Aggregation: `SUM(valor_vendas)`
- Expression: `SUM(`valor_vendas`)`
- Number Format: currency
- Currency Code: BRL

**Configuração JSON**:
```json
{
  "type": "bar",
  "name": "Desempenho do Vendedor",
  "datasets": ["datasets/vendas_base_completa"],
  "xAxis": {
    "column": {"columnName": "vendedor", "columnExpression": "`vendedor`"},
    "sort": {"by": "value", "order": "descending"}
  },
  "yAxis": {
    "expression": "SUM(`valor_vendas`)",
    "numberFormat": "currency",
    "currencyCode": "BRL"
  },
  "position": {"row": 0, "column": 0, "width": 6, "height": 7}
}
```

**Valores Esperados (sem filtros)**:
- Ricardo: R$ 966.266,75
- Raquel: R$ 931.482,68
- ...
- Total: R$ 7.004.651,22

#### Widget 2: Vendas por Região

**Widget Name**: `vendas_por_regiao`  
**Title**: "Vendas por Região"  
**Type**: `bar` (vertical bar chart)  
**Dataset**: `datasets/vendas_base_completa`  
**Position**: Row 0, Column 6  
**Size**: Width 6, Height 7

**xAxis**:
- Column: `regiao`
- Expression: `` `regiao` ``
- Sort: by value, descending

**yAxis**:
- Aggregation: `SUM(valor_vendas)`
- Expression: `SUM(`valor_vendas`)`
- Number Format: currency
- Currency Code: BRL

**Configuração JSON**:
```json
{
  "type": "bar",
  "name": "Vendas por Região",
  "datasets": ["datasets/vendas_base_completa"],
  "xAxis": {
    "column": {"columnName": "regiao", "columnExpression": "`regiao`"},
    "sort": {"by": "value", "order": "descending"}
  },
  "yAxis": {
    "expression": "SUM(`valor_vendas`)",
    "numberFormat": "currency",
    "currencyCode": "BRL"
  },
  "position": {"row": 0, "column": 6, "width": 6, "height": 7}
}
```

**Valores Esperados (sem filtros)**:
- Sul: R$ 1.906.583,44 (maior)
- ...
- Total: R$ 7.004.651,22

#### Widget 3: Vendas por Mês

**Widget Name**: `vendas_por_mes`  
**Title**: "Vendas por Mês"  
**Type**: `line` (line chart)  
**Dataset**: `datasets/vendas_base_completa`  
**Position**: Row 7, Column 0  
**Size**: Width 6, Height 7

**xAxis**:
- Column: `mes_abrev`
- Expression: `` `mes_abrev` ``
- Sort: **NÃO configurar sort no widget!** Ordenação vem do ORDER BY no SQL

**yAxis**:
- Aggregation: `SUM(valor_vendas)`
- Expression: `SUM(`valor_vendas`)`
- Number Format: currency
- Currency Code: BRL

**⚠️ CRÍTICO**: 
- NÃO adicionar `sort` no xAxis - widget ignora sort customizado
- Ordenação JAN→FEV→MAR→ABR→MAI é garantida pelo `ORDER BY ano, mes` no SQL do dataset
- Qualquer tentativa de ordenar no widget resulta em ordem alfabética (ABR, FEV, JAN, MAI, MAR)

**Configuração JSON**:
```json
{
  "type": "line",
  "name": "Vendas por Mês",
  "datasets": ["datasets/vendas_base_completa"],
  "xAxis": {
    "column": {"columnName": "mes_abrev", "columnExpression": "`mes_abrev`"}
  },
  "yAxis": {
    "expression": "SUM(`valor_vendas`)",
    "numberFormat": "currency",
    "currencyCode": "BRL"
  },
  "position": {"row": 7, "column": 0, "width": 6, "height": 7}
}
```

**Ordem Visual Esperada**: JAN → FEV → MAR → ABR → MAI

**Valores Esperados (sem filtros)**:
- JAN: R$ 1.418.303,73
- FEV: R$ 1.214.387,58
- MAR: R$ 1.573.786,55
- ABR: R$ 1.392.062,24
- MAI: R$ 1.406.111,12
- Total: R$ 7.004.651,22

#### Widget 4: Desempenho da Categoria

**Widget Name**: `vendas_por_secao`  
**Title**: "Desempenho da Categoria"  
**Type**: `bar` (horizontal bar chart)  
**Dataset**: `datasets/vendas_base_completa`  
**Position**: Row 7, Column 6  
**Size**: Width 6, Height 7

**xAxis** (eixo horizontal - valores):
- Aggregation: `SUM(valor_vendas)`
- Expression: `SUM(`valor_vendas`)`
- Number Format: currency
- Currency Code: BRL

**yAxis** (eixo vertical - categorias):
- Column: `secao`
- Expression: `` `secao` ``
- Sort: by value, descending

**Observação**: Excel original usa treemap, mas Databricks Lakeview não suporta. Barras horizontais fornecem visualização equivalente.

**Configuração JSON**:
```json
{
  "type": "bar",
  "name": "Desempenho da Categoria",
  "datasets": ["datasets/vendas_base_completa"],
  "xAxis": {
    "expression": "SUM(`valor_vendas`)",
    "numberFormat": "currency",
    "currencyCode": "BRL"
  },
  "yAxis": {
    "column": {"columnName": "secao", "columnExpression": "`secao`"},
    "sort": {"by": "value", "order": "descending"}
  },
  "orientation": "horizontal",
  "position": {"row": 7, "column": 6, "width": 6, "height": 7}
}
```

**Valores Esperados (sem filtros)**:
- Eletrônicos: R$ 1.103.221,37 (maior)
- Telefonia: R$ 1.025.318,19
- ...
- Automotivo: R$ 597.385,97 (menor)
- Total: R$ 7.004.651,22

---

### 3.6 Fase 6: Aplicar Tema Visual

**Paleta de Cores** (azul profissional):

```json
{
  "colors": [
    "#5B9BD5",  // Azul principal
    "#70AD47",  // Verde
    "#FFC000",  // Amarelo
    "#ED7D31",  // Laranja
    "#A5A5A5",  // Cinza
    "#4472C4"   // Azul escuro
  ],
  "canvas": {
    "light": "#E8EEF7",
    "dark": "#1A2332"
  },
  "widgets": {
    "light": "#FFFFFF",
    "dark": "#2D3E50"
  },
  "font": "Arial"
}
```

**Aplicar Tema**:
1. Navegar para Dashboard Settings
2. Theme → Custom Theme
3. Definir paleta de cores acima
4. Configurar background canvas
5. Configurar background de widgets
6. Salvar

---

## 4. Tratamento de Erros

### Exceção 1: Tabela Base Não Existe

**Sintoma**: Dataset retorna erro "Table not found"  
**Causa**: `workspace.vendas_regionais.vendas_base` não foi criada  
**Ação**: Executar `nb_vendas_base_ingestion` (feature VBI) primeiro

### Exceção 2: Tabela Base Vazia

**Sintoma**: Dashboard carrega mas todos os widgets mostram "No data"  
**Causa**: Tabela existe mas não tem registros  
**Ação**: Executar `nb_synthetic_data_generator` + `nb_vendas_base_ingestion`

### Exceção 3: Ordenação Cronológica Quebrada

**Sintoma**: Gráfico de linha mostra ABR, FEV, JAN, MAI, MAR (ordem alfabética)  
**Causa**: Falta `ORDER BY ano, mes` no SQL do dataset  
**Ação**: Editar dataset `vendas_base_completa` e adicionar ORDER BY

### Exceção 4: Filtros Cruzados Não Funcionam

**Sintoma**: Filtrar região não afeta outros widgets  
**Causa**: Widgets usando datasets diferentes  
**Ação**: Garantir que TODOS os widgets usam `datasets/vendas_base_completa`

### Exceção 5: Valores Divergem do Baseline

**Sintoma**: Soma total ≠ R$ 7.004.651,22  
**Causa**: Dados foram regerados ou filtro oculto aplicado  
**Ação**: Limpar todos os filtros e revalidar dados na tabela Delta

---

## 5. Performance

### Volume Esperado

- **Registros**: 1.000 transações
- **Tamanho da Tabela**: ~100 KB (Delta comprimido)
- **Datasets**: 1 único dataset (sem joins complexos)

### Tempo Estimado

- **Dashboard Load**: < 5 segundos
- **Filtro Response**: < 2 segundos
- **Widget Refresh**: < 1 segundo

### Estratégia de Particionamento

**NÃO necessário** para este volume.

Se volume crescer para > 1 milhão de registros:
- Particionar tabela Delta por `ano` e `mes`
- Adicionar filtro de data obrigatório
- Considerar agregações materializadas

### Cache

**NÃO necessário**. Databricks Lakeview gerencia cache automaticamente.

### Otimizações Futuras

1. **Z-Ordering**: `OPTIMIZE vendas_base ZORDER BY (regiao, vendedor)`
2. **Agregações Materializadas**: Views materializadas para períodos históricos
3. **Incremental Refresh**: Atualizar apenas dados novos

---

## 6. Dependências

### Features Upstream

- **synthetic_data_generator**: Gera dados sintéticos
- **vendas_base_ingestion** (VBI): Persiste dados na Delta

### Permissões Requeridas

```sql
-- Unity Catalog permissions
GRANT USE CATALOG ON CATALOG workspace TO `user@domain.com`;
GRANT USE SCHEMA ON SCHEMA vendas_regionais TO `user@domain.com`;
GRANT SELECT ON TABLE vendas_base TO `user@domain.com`;

-- Dashboard permissions
GRANT CAN_VIEW ON DASHBOARD `dashboard_id` TO `user@domain.com`;
GRANT CAN_EDIT ON DASHBOARD `dashboard_id` TO `owner@domain.com`;
```

---

## 7. Testes Requeridos

### Teste 1: Baseline de Dados

**Objetivo**: Validar que soma total está correta  
**Passos**:
1. Abrir dashboard sem filtros
2. Somar valores de TODOS os widgets
3. Validar: Total = R$ 7.004.651,22

**Critério de Sucesso**: ✅ Soma exata

### Teste 2: Filtros Individuais

**Objetivo**: Validar cada filtro isoladamente

| Filtro | Valor | Resultado Esperado |
|--------|-------|--------------------|
| Região | Sul | R$ 1.906.583,44 (250 tx) |
| Mês | MAR | R$ 1.573.786,55 (226 tx) |
| Vendedor | Ricardo | R$ 966.266,75 (131 tx) |
| Trimestre | Q1 | R$ 4.206.477,86 (558 tx) |

**Critério de Sucesso**: ✅ Valores exatos para cada filtro

### Teste 3: Filtros Cruzados

**Objetivo**: Validar que filtros funcionam de forma cruzada

**Passos**:
1. Aplicar Região = Sul
2. Aplicar Mês = MAR
3. Validar: Total = R$ 448.572,29 (61 tx)
4. TODOS os 4 widgets devem mostrar apenas Sul + MAR

**Critério de Sucesso**: ✅ Filtros aplicados em todos os widgets

### Teste 4: Ordenação Cronológica

**Objetivo**: Validar que gráfico de linha mostra meses em ordem

**Passos**:
1. Visualizar widget "Vendas por Mês"
2. Verificar eixo X

**Critério de Sucesso**: ✅ Ordem visual = JAN → FEV → MAR → ABR → MAI

### Teste 5: Formato Monetário

**Objetivo**: Validar formato BRL

**Passos**:
1. Verificar todos os widgets
2. Validar formato: R$ 1.234.567,89

**Critério de Sucesso**: ✅ Símbolo R$, separadores corretos

### Teste 6: Performance

**Objetivo**: Validar tempos de resposta

**Passos**:
1. Carregar dashboard
2. Aplicar filtro
3. Medir tempos

**Critério de Sucesso**: ✅ Load < 5s, Filtro < 2s

### Teste 7: Responsividade

**Objetivo**: Validar layout em diferentes resoluções

**Resoluções Testadas**:
- 1920x1080 (Full HD)
- 1366x768 (Laptop padrão)
- 2560x1440 (2K)

**Critério de Sucesso**: ✅ Layout 2x2 mantido, sem overlap

---

## 8. Versionamento e Documentação

### Definição Exportada

**Path**: `dashboards/dashboard_vendas_regionais.json`  
**Formato**: JSON estruturado  
**Conteúdo**: Metadados, datasets, filtros, widgets, tema

### Documentação Relacionada

- `sdd_instructions.md`: Seção "Dashboards e Visualizações"
- `README.md`: Seção "Dashboard Interativo"
- `plan.md`: Este documento
- `spec.md`: Este arquivo
- `tasks.md`: Checklist de implementação
- `TRACEABILITY_MATRIX.md`: Matriz de rastreabilidade

---

## 9. Problemas Conhecidos e Soluções

### Problema 1: Ordenação de Meses

**Descrição**: Gráfico de linha mostra meses desordenados  
**Solução**: `ORDER BY ano, mes` no SQL do dataset  
**Status**: ✅ Resolvido

### Problema 2: Filtros Cruzados

**Descrição**: Filtros não afetavam todos os widgets  
**Solução**: Usar dataset único `vendas_base_completa`  
**Status**: ✅ Resolvido

### Problema 3: Categorias Desordenadas

**Descrição**: Barras de categoria em ordem alfabética  
**Solução**: `sort: {by: "value", order: "descending"}` no yAxis  
**Status**: ✅ Resolvido

---

**Última Atualização**: 2026-04-19  
**Status**: ✅ Implementado e Validado  
**Próxima Revisão**: 2026-07-19 (trimestral)