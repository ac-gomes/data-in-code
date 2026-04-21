# Projeto: vendas_regionais

**Versão**: 1.2 | **Data**: 2026-04-19 | **Metodologia**: Spec-Driven Development (SDD)

---

## 🆕 Mudanças Recentes (v1.2 - 2026-04-19)

### ✅ Logger Control Localizado
* **Antes**: Logger externo em `/error_handler_logging/`
* **Agora**: Logger local em `src/logger_control`
* **Benefício**: Independência do projeto, sem dependências externas

### ✅ Paths Dinâmicos Implementados
* **Antes**: Paths hard-coded com email (`data.in.code@gmail.com`)
* **Agora**: Paths dinâmicos usando `dbutils.notebook.entry_point.getDbutils()`
* **Exemplo**:
  ```python
  # Código portável
  workspace_base = f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/data-in-code/vendas_regionais"
  ```
* **Benefício**: Código funciona em qualquer workspace Databricks

### ✅ Segurança e Governança
* **Zero exposição de PII**: Nenhum email hard-coded em arquivos versionados
* **Placeholders documentados**: Instrução clara sobre substituição de `<USER_EMAIL>`
* **Dashboard autor**: Atualizado para "ac-gomes" (GitHub username)
* **`.gitignore` criado**: Proteção contra commit de dados sensíveis

### ✅ Notebooks Atualizados
| Notebook | Mudança |
|----------|----------|
| `nb_vendas_base_ingestion.py` | Logger local + paths dinâmicos |
| `ingest_vendas_base.py` | Logger local + paths dinâmicos |
| `nb_synthetic_data_generator.ipynb` | Logger local |
| `nb_create_semantic_views.py` | Logger local |

---

## 📋 Visão Geral

Sistema de ingestão, análise e visualização de vendas regionais a partir de arquivos Excel, com camada semântica e dashboard interativo usando metodologia **Spec-Driven Development (SDD)**.

### Contexto de Origem

Este projeto foi **desenvolvido com Genie Code (Databricks AI Assistant)** a partir de uma necessidade real: automatizar a análise de dados de vendas que originalmente eram processados manualmente em Excel (`VendasRegionaisVBA.xlsm` com macros VBA).

**Desafio inicial**: Migrar análises de Excel para Databricks mantendo funcionalidades e adicionando:
* ✅ Processamento escalável (Delta Lake)
* ✅ Camada semântica (SQL Views)
* ✅ Visualizações interativas (Lakeview Dashboard)
* ✅ Rastreabilidade completa (logging)
* ✅ Documentação rigorosa (SDD)

### Arquitetura

```
Excel → VBI (ingestão) → Delta Table → VSL (semantic layer) → Dashboard Lakeview
                ↓
       Logger Control (local)
```

---

## 🔄 Pipeline de Dados End-to-End

### Visão Geral do Fluxo

```
┌──────────────┐
│ Excel Source │  VendasRegionaisVBA.xlsm
│ (Manual)     │  Aba: "Base"
└──────┬───────┘
       │
       ↓ pandas.read_excel()
┌──────────────────────────────────────────────────────────┐
│ [1] INGESTÃO (VBI)                                       │
│ Notebook: nb_vendas_base_ingestion.py                    │
│ • Lê Excel com Pandas                                    │
│ • Valida e transforma dados                              │
│ • Converte para PySpark DataFrame                        │
│ • Logging completo de operações                          │
└──────┬───────────────────────────────────────────────────┘
       │
       ↓ df.write.saveAsTable()
┌──────────────────────────────────────────────────────────┐
│ [2] DELTA TABLE (Fonte de Verdade)                      │
│ Tabela: workspace.vendas_regionais.vendas_base           │
│ • Formato: Delta Lake                                    │
│ • Localização: Unity Catalog (workspace)                 │
│ • Registros: 1.000 transações                            │
│ • Valor Total: R$ 7.004.651,22                           │
│ • Período: Jan-Mai 2026                                  │
└──────┬───────────────────────────────────────────────────┘
       │
       ↓ spark.table()
┌──────────────────────────────────────────────────────────┐
│ [3] SEMANTIC LAYER (VSL)                                 │
│ Notebook: nb_create_semantic_views.py                    │
│ • Lê da tabela Delta                                     │
│ • Cria 4 temp views SQL agregadas:                       │
│   - vw_vendas_por_vendedor                               │
│   - vw_vendas_por_regiao                                 │
│   - vw_vendas_por_mes                                    │
│   - vw_vendas_por_secao                                  │
│ • Disponibiliza dados para análise SQL                   │
└──────┬───────────────────────────────────────────────────┘
       │
       ↓ SQL Dataset
┌──────────────────────────────────────────────────────────┐
│ [4] DASHBOARD LAKEVIEW                                   │
│ Nome: Dashboard Vendas Regionais                         │
│ • Dataset único: vendas_base_completa                    │
│ • 5 filtros globais (cruzados)                           │
│ • 4 visualizações interativas                            │
│ • Tema: Paleta azul profissional                         │
│ • Funcionalidade: Análise interativa completa            │
└──────────────────────────────────────────────────────────┘
```

---

### [1] Ingestão - VBI (Vendas Base Ingestion)

**Objetivo**: Extrair dados do Excel e persistir em Delta Table.

**Input**:
* **Arquivo**: `arquivos/VendasRegionaisVBA.xlsm`
* **Aba**: "Base"
* **Formato**: Excel com macros VBA (legado)
* **Colunas**: Data da Venda, Região, Vendedor, Código Vendedor, Seção, Vendas, Mês

**Processamento**:
1. **Leitura**: `pandas.read_excel()` - lê aba "Base"
2. **Limpeza**: Remove colunas `Unnamed`, valida schema
3. **Transformação**: 
   - Renomeia colunas para `snake_case`
   - Converte para PySpark DataFrame
   - Adiciona coluna `dt_carga` (timestamp)
4. **Validação**: 
   - Verifica nulos
   - Valida valores positivos
   - Valida dimensões (regiões, meses)
5. **Persistência**: Grava em Delta Table com `overwrite` mode
6. **Logging**: Todas as operações registradas via `LogControl`

**Output**:
* **Tabela Delta**: `workspace.vendas_regionais.vendas_base`
* **Localização**: Unity Catalog (workspace catalog)
* **Modo**: Overwrite (substitui dados anteriores)
* **Schema**:
  ```
  - data_venda: date
  - regiao: string
  - vendedor: string
  - codigo_vendedor: string
  - secao: string
  - valor_vendas: decimal
  - mes_abrev: string
  - ano: integer
  - mes: integer
  - dt_carga: timestamp
  ```

**Como executar**:
```python
# Abrir notebook
/Workspace/Users/<USER_EMAIL>/data-in-code/vendas_regionais/src/nb_vendas_base_ingestion

# Executar todas as células (Run All)
# Tempo estimado: ~30 segundos
```

---

### [2] Delta Table - Fonte de Verdade

**Papel**: Camada de persistência e fonte única de verdade.

**Características**:
* **Formato**: Delta Lake (ACID transactions)
* **Catalog**: Unity Catalog (`workspace` catalog)
* **Tabela**: `workspace.vendas_regionais.vendas_base`
* **Particionamento**: Sem partições (volume de dados pequeno)
* **Versionamento**: Delta Time Travel habilitado
* **Otimização**: Sem Z-Ordering (desnecessário para 1K registros)

**Dados Armazenados**:
| Métrica | Valor |
|---------|-------|
| Registros | 1.000 transações |
| Valor Total | R$ 7.004.651,22 |
| Período | Jan-Mai 2026 |
| Vendedores | 8 únicos |
| Regiões | 4 (Sul, Sudeste, Norte, Nordeste) |
| Seções | 8 categorias |

**Acesso**:
```sql
-- Via SQL
SELECT * FROM workspace.vendas_regionais.vendas_base LIMIT 10;

-- Via PySpark
df = spark.table("workspace.vendas_regionais.vendas_base")
display(df)
```

---

### [3] Semantic Layer - VSL (Vendas Semantic Layer)

**Objetivo**: Criar camada analítica com agregações pré-definidas.

**Input**:
* **Fonte**: Tabela Delta `workspace.vendas_regionais.vendas_base`
* **Método**: `spark.table()` - leitura direta

**Processamento**:
1. **Leitura da Delta Table**:
   ```python
   df = spark.table("workspace.vendas_regionais.vendas_base")
   df.createOrReplaceTempView("vendas_base_temp")
   ```

2. **Criação de 4 Views SQL**:

   **a) `vw_vendas_por_vendedor`**
   - Agregação: Total de vendas, quantidade de transações, ticket médio
   - Agrupamento: Por vendedor
   - Ordenação: Vendas decrescente

   **b) `vw_vendas_por_regiao`**
   - Agregação: Total de vendas, quantidade de transações, quantidade de vendedores
   - Agrupamento: Por região
   - Ordenação: Vendas decrescente

   **c) `vw_vendas_por_mes`**
   - Agregação: Total de vendas, quantidade de transações, vendedores ativos
   - Agrupamento: Por mês (mes_abrev)
   - Ordenação: Cronológica (JAN→FEV→MAR→ABR→MAI)

   **d) `vw_vendas_por_secao`**
   - Agregação: Total de vendas, quantidade de transações, ticket médio
   - Agrupamento: Por seção/categoria
   - Ordenação: Vendas decrescente

**Output**:
* **4 Temp Views SQL** (disponíveis apenas na sessão Spark)
* **Uso**: Análises ad-hoc, queries SQL, dashboards
* **Refresh**: Executar notebook novamente para atualizar

**Como executar**:
```python
# Abrir notebook
/Workspace/Users/<USER_EMAIL>/data-in-code/vendas_regionais/src/nb_create_semantic_views

# Executar todas as células (Run All)
# Tempo estimado: ~15 segundos
```

**Exemplo de uso**:
```sql
-- Top 3 vendedores
SELECT * FROM vw_vendas_por_vendedor LIMIT 3;

-- Vendas por região (ordenado)
SELECT * FROM vw_vendas_por_regiao;

-- Evolução mensal
SELECT * FROM vw_vendas_por_mes;
```

---

### [4] Dashboard Lakeview - Visualização Interativa

**Objetivo**: Interface visual para análise interativa de vendas.

**Input**:
* **Fonte**: Tabela Delta `workspace.vendas_regionais.vendas_base` (diretamente)
* **Dataset**: `vendas_base_completa` (SQL com ORDER BY ano, mes)
* **Nota**: Dashboard **NÃO usa as temp views** - lê diretamente da Delta Table

**Arquitetura do Dashboard**:
```
Delta Table → Dataset SQL (com ORDER BY) → Widgets
                    ↓
            5 Filtros Globais (cruzados)
                    ↓
            4 Visualizações (interativas)
```

**Componentes**:

1. **Dataset Único** (chave para filtros cruzados):
   ```sql
   SELECT 
     data_venda, mes_abrev, regiao, vendedor, 
     codigo_vendedor, secao, valor_vendas, ano, mes
   FROM workspace.vendas_regionais.vendas_base
   ORDER BY ano, mes  -- ⚠️ CRÍTICO para ordenação cronológica!
   ```

2. **5 Filtros Globais** (aplicam-se a todos os widgets):
   - Data da Venda (date range picker)
   - Região (multi-select: Sul, Sudeste, Norte, Nordeste)
   - Mês (multi-select: JAN, FEV, MAR, ABR, MAI)
   - Vendedor (multi-select: 8 vendedores)
   - Trimestre (single-select: Q1, Q2)

3. **4 Visualizações** (layout 2x2):
   - **Desempenho do Vendedor**: Bar chart vertical (vendas por vendedor)
   - **Vendas por Região**: Bar chart vertical (vendas por região)
   - **Vendas por Mês**: Line chart (evolução temporal)
   - **Desempenho da Categoria**: Bar chart horizontal (vendas por seção)

**Características Técnicas**:
* ✅ Filtros cruzados funcionais (todos os widgets usam mesmo dataset)
* ✅ Ordenação cronológica correta (ORDER BY no SQL, não no widget)
* ✅ Agregações nos widgets (SUM, COUNT) - não no SQL
* ✅ Formato monetário BRL em todos os valores
* ✅ Tema visual profissional (paleta azul)

**Como acessar**:
```
1. Databricks UI → Menu lateral → Dashboards
2. Buscar: "Dashboard Vendas Regionais"
3. Ou usar link direto (se disponível)
```

**Baseline de dados** (sem filtros):
| Métrica | Valor |
|---------|-------|
| Total Geral | R$ 7.004.651,22 |
| Transações | 1.000 |
| Vendedores | 8 |
| Regiões | 4 |

---

### Execução do Pipeline Completo

**Ordem de execução**:

```bash
# Passo 1: Ingestão (VBI)
# Notebook: nb_vendas_base_ingestion
# Ação: Run All
# Output: workspace.vendas_regionais.vendas_base (Delta Table)
# Tempo: ~30 segundos

# Passo 2: Semantic Layer (VSL) - OPCIONAL
# Notebook: nb_create_semantic_views
# Ação: Run All
# Output: 4 temp views SQL
# Tempo: ~15 segundos
# Nota: Dashboard NÃO depende das views, lê direto da Delta Table

# Passo 3: Dashboard (visualização)
# UI: Databricks Dashboards → "Dashboard Vendas Regionais"
# Ação: Abrir dashboard
# Automático: Lê da Delta Table e renderiza visualizações
# Tempo: ~3 segundos (load)
```

**Dependências**:
* ✅ VBI → Delta Table (obrigatório para tudo)
* ⚠️ VSL → Temp Views (opcional - apenas para análises SQL ad-hoc)
* ✅ Dashboard → Delta Table (leitura direta, independente das views)

**Frequência de atualização recomendada**:
* **Ingestão (VBI)**: Quando Excel for atualizado (manual ou agendado)
* **Semantic Layer (VSL)**: Sob demanda (apenas se usar as views SQL)
* **Dashboard**: Atualização automática ao abrir (lê Delta Table)

---

### Papel do Genie Code no Desenvolvimento

**Genie Code** (Databricks AI Assistant) foi usado em todas as fases:

1. **Design do Pipeline**:
   - Sugestão da arquitetura Delta Lake + Views + Dashboard
   - Definição da metodologia Spec-Driven Development (SDD)
   - Criação da estrutura de documentação

2. **Desenvolvimento**:
   - Geração de código Python/PySpark para ingestão
   - Criação das queries SQL para semantic layer
   - Configuração do dashboard Lakeview
   - Implementação do sistema de logging (LogControl)

3. **Refinamento**:
   - Debugging de ordenação cronológica no dashboard
   - Correção de filtros cruzados
   - Otimização de paths dinâmicos
   - Sanitização de dados sensíveis (PII)

4. **Documentação**:
   - Geração automática de plan.md, spec.md, tasks.md
   - Criação de matrizes de rastreabilidade
   - Este README.md completo

**Resultado**: Projeto 100% funcional, documentado e reproduzível, desenvolvido colaborativamente com IA.

---

## 🚀 Setup Rápido (Novo Desenvolvedor)

### 1️⃣ Clonar Repositório

```bash
git clone <repo-url> data-in-code
cd data-in-code/vendas_regionais
```

### 2️⃣ Configurar Metodologia Spec-Driven Development (SDD) (OBRIGATÓRIO)

**Copiar template para seu workspace**:

```bash
# Copiar template versionado para escopo do usuário
cp .assistant_instructions.template.md ~/.assistant_instructions.md
```

Ou via Python no Databricks:

```python
import shutil
shutil.copy(
    "/Workspace/Users/<seu_email>/data-in-code/vendas_regionais/.assistant_instructions.template.md",
    "/Workspace/Users/<seu_email>/.assistant_instructions.md"
)
```

**Por quê fazer isso?**
- `.assistant_instructions.md` é lido AUTOMATICAMENTE pelos agentes IA
- Garante que todos seguem mesma metodologia Spec-Driven Development (SDD)
- Cada dev tem sua cópia pessoal (pode personalizar)

### 3️⃣ Substituir Placeholders (⚠️ IMPORTANTE)

Ao usar arquivos JSON de dashboards, **substituir `<USER_EMAIL>` pelo seu email do Databricks**:

```python
# Exemplo: Ler JSON do dashboard
import json

with open('dashboards/dashboard_vendas_regionais.json', 'r') as f:
    config = json.load(f)

# Substituir placeholder
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
config['path_databricks'] = config['path_databricks'].replace('<USER_EMAIL>', user_email)

# Usar config...
```

**Por quê?**
- ✅ Versionamento seguro (sem PII no Git)
- ✅ Portabilidade (funciona para qualquer usuário)
- ✅ Governança (conformidade com políticas de segurança)

### 4️⃣ Explorar Documentação

```
vendas_regionais/
├── .assistant_instructions.template.md  ← Template SDD (copiar para ~/)
├── sdd_instructions.md                  ← Contexto do projeto
├── ARCHITECTURE_FLOW.md                 ← Fluxos visuais
├── AUDIT_REPORT_2026-04-04.md           ← Auditoria completa
├── sdd/
│   └── features/
│       ├── vendas_base_ingestion/       ← VBI (95% conforme)
│       └── vendas_semantic_layer/       ← VSL (95% conforme)
├── src/
│   └── logger_control.py                ← ⭐ Logger local do projeto
└── dashboards/                          ← ⭐ Dashboards versionados
```

**Leitura recomendada** (nesta ordem):
1. `README.md` (este arquivo) ← você está aqui
2. `.assistant_instructions.template.md` → Metodologia Spec-Driven Development (SDD)
3. `sdd_instructions.md` → Contexto do projeto + placeholders
4. `ARCHITECTURE_FLOW.md` → Fluxos e diagramas
5. Features individuais em `sdd/features/{nome}/`
6. Dashboard `dashboards/dashboard_vendas_regionais.json`

---

## 🏭️ Estrutura do Projeto

```
vendas_regionais/
├── .assistant_instructions.template.md  # Template SDD (versionado)
├── .gitignore                           # Proteção de dados sensíveis ⭐ NOVO
├── sdd_instructions.md                  # Contexto local do projeto
├── README.md                            # Este arquivo
├── ARCHITECTURE_FLOW.md                 # Arquitetura híbrida
├── AUDIT_REPORT_2026-04-04.md           # Relatório de auditoria
│
├── src/                                 # Código fonte
│   ├── logger_control.py                # ⭐ Logger local (independente)
│   ├── nb_vendas_base_ingestion.py      # VBI - Ingestão (paths dinâmicos)
│   ├── ingest_vendas_base.py            # VBI - Módulo Python
│   ├── nb_synthetic_data_generator.ipynb # Gerador de dados
│   └── nb_create_semantic_views.py      # VSL - Semantic Layer
│
├── sdd/                                 # Documentação SDD
│   └── features/
│       ├── vendas_base_ingestion/       # VBI - Ingestão Excel
│       │   ├── plan.md
│       │   ├── spec.md
│       │   ├── tasks.md
│       │   └── TRACEABILITY_MATRIX.md
│       │
│       └── vendas_semantic_layer/       # VSL - Views SQL
│           ├── plan.md
│           ├── spec.md
│           ├── tasks.md
│           └── TRACEABILITY_MATRIX.md
│
├── dashboards/                          # ⭐ Dashboards Lakeview
│   └── dashboard_vendas_regionais.json  # Definição versionada
│
└── arquivos/                            # Dados fonte
    └── VendasRegionaisVBA.xlsm
```

---

## 🎯 Features e Assets Implementados

### 1. Logger Control (Local) ⭐ ATUALIZADO
**Status**: ✅ 100% funcional e independente  
**Localização**: `src/logger_control.py`  
**Função**: Logging padronizado para todas as features  
**Benefícios**:
* ✅ Independência do projeto (sem dependências externas)
* ✅ Rastreabilidade completa de operações
* ✅ Error handling padronizado

### 2. vendas_base_ingestion (VBI)
**Status**: ⚠️ 95% conforme Spec-Driven Development (SDD)  
**Função**: Ingestão Excel → Delta Table  
**Localização**: `sdd/features/vendas_base_ingestion/`  
**Notebooks**: 
* `src/nb_vendas_base_ingestion.py` (paths dinâmicos ⭐)
* `src/ingest_vendas_base.py` (módulo Python)
**Tabela**: `workspace.vendas_regionais.vendas_base`  
**Gap**: Apenas testes automatizados pendentes

### 3. vendas_semantic_layer (VSL)
**Status**: ⚠️ 95% conforme Spec-Driven Development (SDD) (MODELO EXEMPLAR)  
**Função**: 4 views SQL analíticas  
**Localização**: `sdd/features/vendas_semantic_layer/`  
**Notebook**: `src/nb_create_semantic_views.py`  
**Views**:
- `vw_vendas_por_regiao`
- `vw_vendas_por_vendedor`
- `vw_vendas_mensais`
- `vw_vendas_secao`  
**Gap**: Apenas testes automatizados pendentes

### 4. Dashboard Vendas Regionais
**Status**: ✅ 100% implementado e funcional  
**Tipo**: Lakeview Dashboard (Databricks)  
**Versão**: 1.0.0  
**Autor**: ac-gomes (GitHub)  
**Data Criação**: 2026-04-19  
**Documentação**: Ver seção [Dashboard Interativo](#-dashboard-interativo) abaixo  
**Definição Exportada**: `dashboards/dashboard_vendas_regionais.json`  

**Características**:
* 📁 Dataset único para filtros cruzados (`vendas_base_completa`)
* 🎯 5 filtros globais (data, região, mês, vendedor, trimestre)
* 📊 4 visualizações (vendedor, região, mês, categoria)
* ✅ Ordenação cronológica correta (via ORDER BY no SQL)
* ✅ Filtros cruzados funcionais
* 🎨 Tema visual profissional (paleta azul)

**Dados**:
* Registros: 1.000 transações
* Valor Total: R$ 7.004.651,22
* Período: Jan-Mai 2026

---

## 📊 Dashboard Interativo

### Visão Geral

Dashboard analítico interativo que replica funcionalidades do Excel `VendasRegionaisVBA.xlsm`, permitindo análise visual de vendas com filtros cruzados globais.

### Identificação

* **Nome**: Dashboard Vendas Regionais
* **ID Databricks**: `01f13c0786871fd189adb02b7e04f008`
* **Autor**: ac-gomes (GitHub)
* **Path**: `/Users/<USER_EMAIL>/Dashboard Vendas Regionais.lvdash.json` (substituir placeholder)
* **Definição Versionada**: `dashboards/dashboard_vendas_regionais.json`

### Arquitetura do Dashboard

**Fonte de Dados**:
```
workspace.vendas_regionais.vendas_base (Delta Table)
           ↓
  Dataset: vendas_base_completa (SQL com ORDER BY)
           ↓
  5 Filtros Globais (cruzados) + 4 Visualizações
```

**Dataset Único** (chave para filtros cruzados):
```sql
SELECT 
  data_venda, mes_abrev, regiao, vendedor, 
  codigo_vendedor, secao, valor_vendas, ano, mes
FROM workspace.vendas_regionais.vendas_base
ORDER BY ano, mes  -- ⚠️ CRÍTICO!
```

### Filtros Globais

| Filtro | Tipo | Valores |
|--------|------|----------|
| Data da Venda | Date Range | Qualquer período |
| Região | Multi-select | 4 regiões (Sul, Nordeste, Sudeste, Norte) |
| Mês | Multi-select | 5 meses (JAN, FEV, MAR, ABR, MAI) |
| Vendedor | Multi-select | 8 vendedores |
| Trimestre | Single-select | Q1, Q2 |

### Visualizações (Layout 2x2)

1. **Desempenho do Vendedor** - Bar chart vertical (vendas por vendedor)
2. **Vendas por Região** - Bar chart vertical (vendas por região)
3. **Vendas por Mês** - Line chart (evolução temporal JAN→MAI)
4. **Desempenho da Categoria** - Bar chart horizontal (vendas por seção)

### Lições Aprendidas 💡

**Problema 1**: Meses desordenados no gráfico de linha  
**Solução**: Adicionar `ORDER BY ano, mes` no SQL do dataset (não no widget)

**Problema 2**: Filtros cruzados não funcionavam  
**Solução**: Usar dataset único para todos os widgets (agregar nos widgets, não no SQL)

**Documentação Completa**: Ver seção "Dashboards e Visualizações" em `sdd_instructions.md`

### Baseline de Dados (Sem Filtros)

| Métrica | Valor |
|---------|-------|
| Total Geral | R$ 7.004.651,22 |
| Transações | 1.000 |
| Vendedores | 8 |
| Regiões | 4 |
| Meses | 5 |
| Seções | 8 |

---

## 🔒 Segurança e Governança ⭐ NOVO

### Placeholders e PII

**Regra**: Arquivos versionados **NÃO devem conter PII** (dados pessoais identificáveis).

**Placeholder `<USER_EMAIL>`**:
* Usado em: `dashboards/dashboard_vendas_regionais.json`
* **Ação requerida**: Substituir pelo email real do usuário ao usar o arquivo
* **Exemplo**:
  ```python
  user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
  path = f"/Users/{user_email}/dashboard.lvdash.json"
  ```

### Paths Dinâmicos

**Todos os notebooks usam paths dinâmicos** (sem hard-coding de email):

```python
# ✅ CORRETO - Path dinâmico
workspace_base = f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/data-in-code/vendas_regionais"
excel_path = f"{workspace_base}/arquivos/VendasRegionaisVBA.xlsm"

# ❌ ERRADO - Path hard-coded (antigo)
# excel_path = "/Workspace/Users/data.in.code@gmail.com/..."
```

### `.gitignore` Criado

Arquivo `.gitignore` protege contra commit acidental de:
* Credenciais e tokens
* Logs com dados sensíveis
* Arquivos com PII
* Dados não versionados

**Status**: ✅ Zero exposição de PII em arquivos versionados

---

## 📊 Conformidade Spec-Driven Development (SDD)

| Feature/Asset | Conformidade | Gap Único |
| --- | --- | --- |
| Logger Control (local) | 100% ✅ | Nenhum |
| vendas_base_ingestion | 95% ⚠️ | Testes |
| vendas_semantic_layer | 95% ⚠️ | Testes |
| Dashboard Vendas Regionais | 100% ✅ | Nenhum |
| Segurança (PII) | 100% ✅ | Nenhum |
| **Média do Projeto** | **95%** | Testes automatizados |

---

## 🔧 Como Trabalhar Neste Projeto

### Adicionar Nova Feature

1. **Criar estrutura SDD**:
   ```
   sdd/features/nova_feature/
   ├── plan.md
   ├── spec.md
   ├── tasks.md
   ├── TRACEABILITY_MATRIX.md
   ├── src/
   └── tests/
   ```

2. **Definir feature code** (3 letras):
   - Exemplo: `NFT` para "nova_feature_teste"

3. **Seguir workflow Spec-Driven Development (SDD)** (4 fases):
   - Identificação → Leitura → Implementação → Documentação

4. **Usar LogControl local** (obrigatório):
   ```python
   %run ./logger_control  # Logger local do projeto
   logger = LogControl(logger_name="nova_feature", ...)
   ```

5. **Usar paths dinâmicos**:
   ```python
   workspace_base = f"/Workspace/Users/{dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()}/data-in-code/vendas_regionais"
   ```

6. **Atualizar documentação**:
   - Matriz de rastreabilidade
   - tasks.md
   - sdd_instructions.md (adicionar feature à lista)

### Criar Novo Dashboard

1. Criar dashboard via UI Databricks
2. Exportar definição: Buscar com `searchAssets`, ler com `readAssetById`
3. Criar arquivo JSON: `dashboards/dashboard_<nome>.json`
4. **Substituir emails por `<USER_EMAIL>` placeholder**
5. Documentar no `sdd_instructions.md` (seção "Dashboards e Visualizações")
6. Incluir ID, autor (GitHub username), datasets, filtros, widgets, lições aprendidas
7. Atualizar este README

### Modificar Feature Existente

1. Ler documentação da feature (plan/spec/tasks)
2. Verificar matriz de rastreabilidade (gaps conhecidos)
3. Implementar seguindo padrões Spec-Driven Development (SDD)
4. Atualizar matriz e tasks.md

---

## 🛠️ Ferramentas e Dependências

### Linguagens
- Python 3.x
- SQL (Databricks SQL)

### Bibliotecas Python
- pandas
- openpyxl
- PySpark

### Infraestrutura
- Databricks Workspace
- Unity Catalog (workspace catalog)
- Delta Lake
- Lakeview Dashboards

---

## 📚 Documentação

### Principais Documentos

| Documento | Descrição |
| --- | --- |
| [README.md](#) | Este arquivo |
| [sdd_instructions.md](sdd_instructions.md) | Contexto específico do projeto + placeholders |
| [ARCHITECTURE_FLOW.md](ARCHITECTURE_FLOW.md) | Arquitetura híbrida e fluxos |
| [AUDIT_REPORT_2026-04-04.md](AUDIT_REPORT_2026-04-04.md) | Auditoria completa do projeto |
| [.assistant_instructions.template.md](.assistant_instructions.template.md) | Template SDD (copiar para ~/.) |
| [.gitignore](.gitignore) | Proteção de dados sensíveis ⭐ NOVO |

### Features (plan/spec/tasks/matriz)

| Feature | Documentação |
| --- | --- |
| vendas_base_ingestion | [sdd/features/vendas_base_ingestion/](sdd/features/vendas_base_ingestion/) |
| vendas_semantic_layer | [sdd/features/vendas_semantic_layer/](sdd/features/vendas_semantic_layer/) |

### Dashboards

| Dashboard | Definição | Documentação |
| --- | --- | --- |
| Dashboard Vendas Regionais | [dashboards/dashboard_vendas_regionais.json](dashboards/dashboard_vendas_regionais.json) | [sdd_instructions.md](sdd_instructions.md#dashboards-e-visualizações) |

---

## 🤝 Trabalho em Equipe

### Setup para Novos Membros

1. **Clonar repositório**
2. **Copiar template** (`.assistant_instructions.template.md` → `~/.assistant_instructions.md`)
3. **Ler documentação** (ordem: README → template → sdd_instructions → features)
4. **Entender placeholders** (`<USER_EMAIL>` deve ser substituído)
5. **Explorar modelo exemplar** (vendas_semantic_layer)
6. **Acessar dashboard** (Dashboard Vendas Regionais)

### Atualizações de Metodologia

Quando a metodologia Spec-Driven Development (SDD) for atualizada:

1. **Atualizar template**:
   ```bash
   # Editar .assistant_instructions.template.md
   git add .assistant_instructions.template.md
   git commit -m "feat: atualizar metodologia Spec-Driven Development (SDD)"
   git push
   ```

2. **Notificar equipe** para atualizar suas cópias pessoais:
   ```bash
   # Cada dev executa:
   cp .assistant_instructions.template.md ~/.assistant_instructions.md
   ```

---

## 📈 Status do Projeto

| Aspecto | Status |
| --- | --- |
| **Documentação SDD** | ✅ 100% completa |
| **Features implementadas** | 2/2 (100%) |
| **Dashboard implementado** | ✅ 1/1 (100%) |
| **Logger Control** | ✅ Local e independente ⭐ |
| **Paths dinâmicos** | ✅ Implementados ⭐ |
| **Segurança (PII)** | ✅ Zero exposição ⭐ |
| **Conformidade SDD** | 95% (testes pendentes) |
| **Rastreabilidade** | ✅ Implementada |
| **Versionamento Dashboard** | ✅ Implementado (JSON) |
| **Testes automatizados** | ❌ 0% (gap identificado) |
| **Auditoria** | ✅ Concluída (2026-04-04) |
| **Auditoria Segurança** | ✅ Concluída (2026-04-19) ⭐ |

---

## 🔍 Referências Rápidas

### Comando Git Clone
```bash
git clone <repo-url> data-in-code
```

### Setup Spec-Driven Development (SDD)
```bash
cp data-in-code/vendas_regionais/.assistant_instructions.template.md ~/.assistant_instructions.md
```

### Abrir Notebooks
- **Logger Control**: `src/logger_control.py`
- **Ingestão**: `src/nb_vendas_base_ingestion.py` (paths dinâmicos)
- **Semantic Layer**: `src/nb_create_semantic_views.py`

### Dashboards
- **Dashboard Vendas Regionais**: UI Databricks → Dashboards → "Dashboard Vendas Regionais"
- **Definição JSON**: `dashboards/dashboard_vendas_regionais.json` (substituir `<USER_EMAIL>`)

### Feature Codes
- **VBI** = vendas_base_ingestion
- **VSL** = vendas_semantic_layer
- **DVR** = Dashboard Vendas Regionais

---

## 📞 Contato

**Mantenedor**: https://github.com/ac-gomes  
**Metodologia**: Spec-Driven Development (SDD)  
**Última Atualização**: 2026-04-19 (v1.2)

---

**🎯 Próximos Passos Sugeridos**:
1. ✅ Setup Spec-Driven Development (SDD) (copiar template)
2. 📖 Ler sdd_instructions.md (com seção de placeholders)
3. 🔄 Entender o pipeline end-to-end (Excel → Dashboard)
4. 🔍 Explorar vendas_semantic_layer (modelo exemplar)
5. 📊 Acessar Dashboard Vendas Regionais (interativo)
6. 🔒 Entender placeholders e paths dinâmicos
7. 🚀 Começar a trabalhar!