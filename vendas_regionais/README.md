# Projeto: vendas_regionais

**Versão**: 1.0 | **Data**: 2026-04-04 | **Metodologia**: Spec-Driven Development (SDD)

---

## 📋 Visão Geral

Sistema de ingestão e análise de vendas regionais a partir de arquivos Excel, com camada semântica para análise usando metodologia **Spec-Driven Development (SDD)**.

### Arquitetura

```
Excel → VBI (ingestão) → Delta Table → VSL (semantic layer) → Dashboards
                ↓
              EHL (logging infraestrutura)
```

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

### 3️⃣ Explorar Documentação

```
vendas_regionais/
├── .assistant_instructions.template.md  ← Template SDD (copiar para ~/)
├── sdd_instructions.md                  ← Contexto do projeto
├── ARCHITECTURE_FLOW.md                 ← Fluxos visuais
├── AUDIT_REPORT_2026-04-04.md           ← Auditoria completa
├── sdd/
│   └── features/
│       ├── error_handler_logging/      ← EHL (100% conforme)
│       ├── vendas_base_ingestion/      ← VBI (95% conforme)
│       └── vendas_semantic_layer/      ← VSL (95% conforme)
```

**Leitura recomendada** (nesta ordem):
1. `README.md` (este arquivo) ← você está aqui
2. `.assistant_instructions.template.md` → Metodologia Spec-Driven Development (SDD)
3. `sdd_instructions.md` → Contexto do projeto
4. `ARCHITECTURE_FLOW.md` → Fluxos e diagramas
5. Features individuais em `sdd/features/{nome}/`

---

## 🏭️ Estrutura do Projeto

```
vendas_regionais/
├── .assistant_instructions.template.md  # Template SDD (versionado)
├── sdd_instructions.md                  # Contexto local do projeto
├── README.md                            # Este arquivo
├── ARCHITECTURE_FLOW.md                 # Arquitetura híbrida
├── AUDIT_REPORT_2026-04-04.md           # Relatório de auditoria
│
├── sdd/                                 # Documentação SDD
│   └── features/
│       ├── error_handler_logging/       # EHL - Logging
│       │   ├── plan.md
│       │   ├── spec.md
│       │   ├── tasks.md
│       │   ├── TRACEABILITY_MATRIX.md
│       │   └── src/logger_control
│       │
│       ├── vendas_base_ingestion/       # VBI - Ingestão Excel
│       │   ├── plan.md
│       │   ├── spec.md
│       │   ├── tasks.md
│       │   ├── TRACEABILITY_MATRIX.md
│       │   └── src/ingest_vendas_base
│       │
│       └── vendas_semantic_layer/       # VSL - Views SQL
│           ├── plan.md
│           ├── spec.md
│           ├── tasks.md
│           ├── TRACEABILITY_MATRIX.md
│           └── src/nb_create_semantic_views
│
└── arquivos/                            # Dados fonte
    └── VendasRegionaisVBA.xlsm
```

---

## 🎯 Features Implementadas

### 1. error_handler_logging (EHL)
**Status**: ✅ 100% conforme Spec-Driven Development (SDD)  
**Função**: Infraestrutura de logging para todas as features  
**Localização**: `sdd/features/error_handler_logging/`  
**Notebook**: `src/logger_control`

### 2. vendas_base_ingestion (VBI)
**Status**: ⚠️ 95% conforme Spec-Driven Development (SDD)  
**Função**: Ingestão Excel → Delta Table  
**Localização**: `sdd/features/vendas_base_ingestion/`  
**Notebook**: `src/ingest_vendas_base`  
**Tabela**: `hive_metastore.vendas_regionais.vendas_base`  
**Gap**: Apenas testes automatizados pendentes

### 3. vendas_semantic_layer (VSL)
**Status**: ⚠️ 95% conforme Spec-Driven Development (SDD) (MODELO EXEMPLAR)  
**Função**: 4 views SQL analíticas  
**Localização**: `sdd/features/vendas_semantic_layer/`  
**Notebook**: `src/nb_create_semantic_views`  
**Views**:
- `vw_vendas_por_regiao`
- `vw_vendas_por_vendedor`
- `vw_vendas_mensais`
- `vw_vendas_secao`  
**Gap**: Apenas testes automatizados pendentes

---

## 📊 Conformidade Spec-Driven Development (SDD)

| Feature | Conformidade | Gap Único |
| --- | --- | --- |
| error_handler_logging | 100% ✅ | Nenhum |
| vendas_base_ingestion | 95% ⚠️ | Testes |
| vendas_semantic_layer | 95% ⚠️ | Testes |
| **Média do Projeto** | **90%** | Testes automatizados |

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

4. **Usar LogControl** (obrigatório):
   ```python
   %run ../../error_handler_logging/src/logger_control
   logger = LogControl(logger_name="nova_feature", ...)
   ```

5. **Atualizar documentação**:
   - Matriz de rastreabilidade
   - tasks.md
   - sdd_instructions.md (adicionar feature à lista)

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
- Unity Catalog (opcional, atualmente usa hive_metastore)
- Delta Lake

---

## 📚 Documentação

### Principais Documentos

| Documento | Descrição |
| --- | --- |
| [README.md](#) | Este arquivo |
| [sdd_instructions.md](sdd_instructions.md) | Contexto específico do projeto |
| [ARCHITECTURE_FLOW.md](ARCHITECTURE_FLOW.md) | Arquitetura híbrida e fluxos |
| [AUDIT_REPORT_2026-04-04.md](AUDIT_REPORT_2026-04-04.md) | Auditoria completa do projeto |
| [.assistant_instructions.template.md](.assistant_instructions.template.md) | Template SDD (copiar para ~/.) |

### Features (plan/spec/tasks/matriz)

| Feature | Documentação |
| --- | --- |
| error_handler_logging | [sdd/features/error_handler_logging/](sdd/features/error_handler_logging/) |
| vendas_base_ingestion | [sdd/features/vendas_base_ingestion/](sdd/features/vendas_base_ingestion/) |
| vendas_semantic_layer | [sdd/features/vendas_semantic_layer/](sdd/features/vendas_semantic_layer/) |

---

## 🤝 Trabalho em Equipe

### Setup para Novos Membros

1. **Clonar repositório**
2. **Copiar template** (`.assistant_instructions.template.md` → `~/.assistant_instructions.md`)
3. **Ler documentação** (ordem: README → template → sdd_instructions → features)
4. **Explorar modelo exemplar** (vendas_semantic_layer)

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
| **Features implementadas** | 3/3 (100%) |
| **Conformidade SDD** | 90% (testes pendentes) |
| **LogControl** | ✅ Implementado |
| **Rastreabilidade** | ✅ Implementada |
| **Testes automatizados** | ❌ 0% (gap identificado) |
| **Auditoria** | ✅ Concluída (2026-04-04) |

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
- **LogControl**: `sdd/features/error_handler_logging/src/logger_control`
- **Ingestão**: `sdd/features/vendas_base_ingestion/src/ingest_vendas_base`
- **Semantic Layer**: `sdd/features/vendas_semantic_layer/src/nb_create_semantic_views`

### Feature Codes
- **EHL** = error_handler_logging
- **VBI** = vendas_base_ingestion
- **VSL** = vendas_semantic_layer

---

## 📞 Contato

**Mantenedor**: https://github.com/ac-gomes  
**Metodologia**: Spec-Driven Development (SDD)  
**Última Atualização**: 2026-04-04

---

**🎯 Próximos Passos Sugeridos**:
1. ✅ Setup Spec-Driven Development (SDD) (copiar template)
2. 📖 Ler sdd_instructions.md
3. 🔍 Explorar vendas_semantic_layer (modelo exemplar)
4. 🚀 Começar a trabalhar!
