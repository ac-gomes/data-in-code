# Projeto: vendas_regionais

**Desenvolvido com**: Genie Code (Databricks AI Assistant)  
**Origem**: Migração de análise manual Excel para pipeline automatizado  
**Metodologia**: Spec-Driven Development (SDD)

---

## 📋 Sobre o Projeto

Sistema automatizado de análise de vendas regionais, desenvolvido **colaborativamente com Genie Code** (Databricks AI Assistant).

**Origem**: Planilha Excel (`VendasRegionaisVBA.xlsm`) com análises manuais e macros VBA foi transformada em pipeline completo com:

* ✅ **Ingestão automatizada** - Leitura de Excel para Delta Lake
* ✅ **Camada analítica** - Views SQL agregadas (vendas por região, vendedor, mês, categoria)
* ✅ **Dashboard interativo** - Lakeview Dashboard com filtros cruzados
* ✅ **Logging padronizado** - Rastreabilidade completa de operações
* ✅ **Documentação rigorosa** - Seguindo metodologia Spec-Driven Development (SDD)

---

## 🔄 Pipeline End-to-End

```
Excel (VendasRegionaisVBA.xlsm)
       ↓
[1] Ingestão (Python/PySpark)
       ↓
Delta Table (workspace.vendas_regionais.vendas_base)
       ↓
[2] Semantic Layer (SQL Views)
       ↓
Dashboard Lakeview (Visualizações Interativas)
```

**Dados Sintéticos**:

O projeto utiliza **dados sintéticos** gerados automaticamente para demonstração e testes. Um gerador de dados (`nb_synthetic_data_generator.ipynb`) cria transações realistas com dimensões representativas:

* 1.000 transações de vendas
* 8 vendedores distintos
* 4 regiões geográficas (Sul, Sudeste, Norte, Nordeste)
* 5 meses de histórico (Jan-Mai 2026)
* 8 categorias de produtos (seções)

Por serem dados sintéticos, os valores monetários não refletem cenários reais e servem apenas para validação do pipeline e visualizações.

---

## 🚀 Como Usar

### 1️⃣ Clonar Repositório

```bash
git clone <repo-url> data-in-code
cd data-in-code/vendas_regionais
```

### 2️⃣ Configurar Template SDD (Opcional)

Para que os agentes IA sigam a metodologia Spec-Driven Development (SDD) do projeto:

```bash
# Copiar template para seu workspace
cp user_global_instructions/.assistant_instructions.md ~/.assistant_instructions.md
```

Ou via Databricks:

```python
import shutil
shutil.copy(
    "/Workspace/Users/{username}/data-in-code/vendas_regionais/user_global_instructions/.assistant_instructions.md",
    "/Workspace/Users/{username}/.assistant_instructions.md"
)
```

### 3️⃣ Executar Pipeline

#### Passo 1: Ingestão (Obrigatório)

**Notebook**: `src/nb_vendas_base_ingestion.py`

1. Abrir notebook no Databricks
2. Executar todas as células (Run All)
3. Aguardar ~30 segundos

**Output**: Tabela Delta `workspace.vendas_regionais.vendas_base` criada

#### Passo 2: Semantic Layer (Opcional)

**Notebook**: `src/nb_create_semantic_views.py`

1. Abrir notebook no Databricks
2. Executar todas as células (Run All)
3. Aguardar ~15 segundos

**Output**: 4 temp views SQL disponíveis para análise
* `vw_vendas_por_vendedor`
* `vw_vendas_por_regiao`
* `vw_vendas_por_mes`
* `vw_vendas_por_secao`

#### Passo 3: Dashboard (Visualização)

1. Databricks UI → Menu lateral → **Dashboards**
2. Buscar: "Dashboard Vendas Regionais"
3. Explorar visualizações interativas com filtros cruzados

---

## 📁 Estrutura do Projeto

```
vendas_regionais/
├── README.md                                  # Este arquivo
├── sdd_instructions.md                        # Contexto e padrões do projeto
├── user_global_instructions/                  # Template SDD (para agentes IA)
│   └── .assistant_instructions.md
│
├── src/                                       # Código fonte
│   ├── logger_control.py                      # Logger padronizado
│   ├── nb_vendas_base_ingestion.py            # [1] Ingestão Excel → Delta
│   ├── nb_create_semantic_views.py            # [2] Views SQL agregadas
│   └── nb_synthetic_data_generator.ipynb      # Gerador de dados sintéticos
│
├── sdd/                                       # Documentação SDD
│   └── features/
│       ├── vendas_base_ingestion/             # Docs: Ingestão
│       └── vendas_semantic_layer/             # Docs: Semantic Layer
│
├── dashboards/                                # Dashboards versionados
│   └── dashboard_vendas_regionais.json        # Definição do dashboard
│
├── arquivos/                                  # Dados fonte
│   └── VendasRegionaisVBA.xlsm                # Excel original
│
└── tests/                                     # Testes (em desenvolvimento)
```

---

## 🎯 Features Implementadas

| Feature | Status | Descrição |
|---------|--------|------------|
| **Logger Control** | ✅ 100% | Logging padronizado para rastreabilidade |
| **Ingestão (VBI)** | ✅ 95% | Excel → Delta Table (testes pendentes) |
| **Semantic Layer (VSL)** | ✅ 95% | 4 views SQL agregadas (testes pendentes) |
| **Dashboard Lakeview** | ✅ 100% | 5 filtros + 4 visualizações interativas |

---

## 🔒 Segurança e Portabilidade

### Paths Dinâmicos

Todos os notebooks usam **paths dinâmicos** - funcionam em qualquer workspace:

```python
# Código portável (sem email hard-coded)
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
workspace_base = f"/Workspace/Users/{username}/data-in-code/vendas_regionais"
excel_path = f"{workspace_base}/arquivos/VendasRegionaisVBA.xlsm"
```

### Placeholder `{username}`

Arquivos versionados (JSON de dashboards) usam placeholder:

```python
# Substituir ao usar
user_email = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
config['path'] = config['path'].replace('{username}', user_email)
```

**Benefício**: ✅ Zero PII no repositório Git, funciona para qualquer usuário.

---

## 📚 Documentação Adicional

| Documento | Descrição |
|-----------|------------|
| `sdd_instructions.md` | Contexto específico do projeto, padrões SDD |
| `ARCHITECTURE_FLOW.md` | Diagramas detalhados da arquitetura |
| `AUDIT_REPORT_2026-04-04.md` | Relatório de auditoria completo |
| `sdd/features/*/` | Documentação SDD por feature (plan, spec, tasks, matriz) |

---

## 🤝 Papel do Genie Code

**Genie Code** (Databricks AI Assistant) foi utilizado em todas as fases:

* 🏗️ **Design**: Arquitetura Delta Lake + Views + Dashboard
* 💻 **Desenvolvimento**: Código Python/PySpark, queries SQL, dashboard Lakeview
* 🐛 **Debugging**: Filtros cruzados, ordenação cronológica, paths dinâmicos
* 📖 **Documentação**: Geração automática de specs, tasks, matrizes, este README

**Resultado**: Projeto 100% funcional, documentado e reproduzível desenvolvido colaborativamente com IA.

---

## 📈 Status Atual

| Aspecto | Status |
|---------|--------|
| **Ingestão** | ✅ Funcional |
| **Semantic Layer** | ✅ Funcional |
| **Dashboard** | ✅ Funcional |
| **Documentação SDD** | ✅ Completa |
| **Testes Automatizados** | ⚠️ Pendente |
| **Segurança (PII)** | ✅ Zero exposição |

---

**🎯 Próximos Passos Sugeridos**:

1. Clone o repositório
2. Execute notebook de ingestão (`src/nb_vendas_base_ingestion.py`)
3. Explore o Dashboard Lakeview
4. Leia `sdd_instructions.md` para entender o contexto completo
