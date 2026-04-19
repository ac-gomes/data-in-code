# Tasks: Dashboard Vendas Regionais (DVR)

**Feature Code**: DVR  
**Versão**: 1.0.0  
**Data**: 2026-04-19  
**Status**: ✅ 100% Completo

---

## Status Geral

* **Total de Tasks**: 65
* **Concluídas**: 65 (✅)
* **Em Progresso**: 0
* **Pendentes**: 0

---

## Fase 1: Pré-requisitos e Validação

### 1.1 Validar Dependências

* [x] **TASK-DVR-001**: Validar que tabela `workspace.vendas_regionais.vendas_base` existe
* [x] **TASK-DVR-002**: Validar que tabela contém ~1.000 registros
* [x] **TASK-DVR-003**: Validar que soma total = R$ 7.004.651,22
* [x] **TASK-DVR-004**: Validar que período de dados = Jan-Mai 2026
* [x] **TASK-DVR-005**: Validar permissões SELECT na tabela

### 1.2 Preparar Ambiente

* [x] **TASK-DVR-006**: Abrir Databricks Workspace
* [x] **TASK-DVR-007**: Navegar para seção Dashboards
* [x] **TASK-DVR-008**: Verificar permissões para criar dashboard

---

## Fase 2: Criação do Dashboard

### 2.1 Criar Dashboard Vazio

* [x] **TASK-DVR-009**: Clicar em "Create Dashboard"
* [x] **TASK-DVR-010**: Selecionar "Lakeview Dashboard" (não SQL Dashboard)
* [x] **TASK-DVR-011**: Nomear dashboard: "Dashboard Vendas Regionais"
* [x] **TASK-DVR-012**: Confirmar criação
* [x] **TASK-DVR-013**: Anotar Dashboard ID (01f13c0786871fd189adb02b7e04f008)
* [x] **TASK-DVR-014**: Anotar TreeNode ID (4064538371365942)

---

## Fase 3: Criar Dataset Base

### 3.1 Configurar Dataset

* [x] **TASK-DVR-015**: Clicar em "Add Dataset"
* [x] **TASK-DVR-016**: Nomear dataset: `vendas_base_completa`
* [x] **TASK-DVR-017**: Nomear display name: "Vendas Base Completa"
* [x] **TASK-DVR-018**: Selecionar tipo: "SQL Query"

### 3.2 Escrever Query SQL

* [x] **TASK-DVR-019**: Adicionar SELECT com 9 colunas (data_venda, mes_abrev, regiao, vendedor, codigo_vendedor, secao, valor_vendas, ano, mes)
* [x] **TASK-DVR-020**: Adicionar FROM workspace.vendas_regionais.vendas_base
* [x] **TASK-DVR-021**: ⚠️ **CRÍTICO**: Adicionar `ORDER BY ano, mes` (sem isso, ordenação cronológica quebra!)
* [x] **TASK-DVR-022**: Executar query para testar
* [x] **TASK-DVR-023**: Validar que retorna ~1.000 registros
* [x] **TASK-DVR-024**: Validar que meses estão ordenados cronologicamente (JAN→FEV→MAR→ABR→MAI)
* [x] **TASK-DVR-025**: Salvar dataset

### 3.3 Adicionar Colunas Calculadas

* [x] **TASK-DVR-026**: Adicionar coluna calculada "Trimestre"
* [x] **TASK-DVR-027**: Expressão: `CASE WHEN mes IN (1,2,3) THEN 'Q1' WHEN mes IN (4,5,6) THEN 'Q2' ...`
* [x] **TASK-DVR-028**: Validar que retorna Q1 e Q2
* [x] **TASK-DVR-029**: (Opcional) Adicionar coluna "Ordem Mês" (CASE mes_abrev WHEN 'JAN' THEN 1...)
* [x] **TASK-DVR-030**: (Opcional) Adicionar coluna "Mês Ordenado" (CONCAT(LPAD...))

---

## Fase 4: Criar Página "Global Filters"

### 4.1 Criar Página de Filtros

* [x] **TASK-DVR-031**: Clicar em "Add Page"
* [x] **TASK-DVR-032**: Nomear página: "Global Filters"
* [x] **TASK-DVR-033**: Configurar como página de filtros globais

### 4.2 Adicionar Filtro: Data da Venda

* [x] **TASK-DVR-034**: Adicionar widget tipo "Date Range Filter"
* [x] **TASK-DVR-035**: Nomear: "Data da Venda"
* [x] **TASK-DVR-036**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-037**: Selecionar coluna `data_venda`
* [x] **TASK-DVR-038**: Posicionar em (row:0, col:0, width:3, height:4)
* [x] **TASK-DVR-039**: Salvar filtro

### 4.3 Adicionar Filtro: Região

* [x] **TASK-DVR-040**: Adicionar widget tipo "Multi-select Filter"
* [x] **TASK-DVR-041**: Nomear: "Região"
* [x] **TASK-DVR-042**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-043**: Selecionar coluna `regiao`
* [x] **TASK-DVR-044**: Ativar "Allow Multiple Selection"
* [x] **TASK-DVR-045**: Posicionar em (row:0, col:3, width:3, height:4)
* [x] **TASK-DVR-046**: Validar que mostra 4 regiões (Sul, Nordeste, Sudeste, Norte)
* [x] **TASK-DVR-047**: Salvar filtro

### 4.4 Adicionar Filtro: Mês

* [x] **TASK-DVR-048**: Adicionar widget tipo "Multi-select Filter"
* [x] **TASK-DVR-049**: Nomear: "Mês"
* [x] **TASK-DVR-050**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-051**: Selecionar coluna `mes_abrev`
* [x] **TASK-DVR-052**: Ativar "Allow Multiple Selection"
* [x] **TASK-DVR-053**: Posicionar em (row:0, col:6, width:3, height:4)
* [x] **TASK-DVR-054**: Validar que mostra 5 meses (JAN, FEV, MAR, ABR, MAI)
* [x] **TASK-DVR-055**: Salvar filtro

### 4.5 Adicionar Filtro: Vendedor

* [x] **TASK-DVR-056**: Adicionar widget tipo "Multi-select Filter"
* [x] **TASK-DVR-057**: Nomear: "Vendedor"
* [x] **TASK-DVR-058**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-059**: Selecionar coluna `vendedor`
* [x] **TASK-DVR-060**: Ativar "Allow Multiple Selection"
* [x] **TASK-DVR-061**: Posicionar em (row:0, col:9, width:3, height:4)
* [x] **TASK-DVR-062**: Validar que mostra 8 vendedores
* [x] **TASK-DVR-063**: Salvar filtro

### 4.6 Adicionar Filtro: Trimestre

* [x] **TASK-DVR-064**: Adicionar widget tipo "Single-select Filter"
* [x] **TASK-DVR-065**: Nomear: "Trimestre"
* [x] **TASK-DVR-066**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-067**: Selecionar coluna calculada `Trimestre`
* [x] **TASK-DVR-068**: Ativar "Allow Multiple Selection"
* [x] **TASK-DVR-069**: Posicionar em (row:4, col:0, width:3, height:4)
* [x] **TASK-DVR-070**: Validar que mostra Q1 e Q2
* [x] **TASK-DVR-071**: Salvar filtro

---

## Fase 5: Criar Página de Visualizações

### 5.1 Criar Página Principal

* [x] **TASK-DVR-072**: Clicar em "Add Page"
* [x] **TASK-DVR-073**: Nomear página: "Page 1"
* [x] **TASK-DVR-074**: Configurar layout como grid 12x14

### 5.2 Widget 1: Desempenho do Vendedor

* [x] **TASK-DVR-075**: Adicionar widget tipo "Bar Chart"
* [x] **TASK-DVR-076**: Nomear: "Desempenho do Vendedor"
* [x] **TASK-DVR-077**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-078**: Configurar xAxis: coluna `vendedor`
* [x] **TASK-DVR-079**: Configurar yAxis: agregação `SUM(valor_vendas)`
* [x] **TASK-DVR-080**: Configurar sort: by value, descending
* [x] **TASK-DVR-081**: Configurar formato: currency, BRL
* [x] **TASK-DVR-082**: Posicionar em (row:0, col:0, width:6, height:7)
* [x] **TASK-DVR-083**: Validar que mostra 8 vendedores ordenados por vendas (maior→menor)
* [x] **TASK-DVR-084**: Validar formato monetário: R$ 1.234.567,89
* [x] **TASK-DVR-085**: Salvar widget

### 5.3 Widget 2: Vendas por Região

* [x] **TASK-DVR-086**: Adicionar widget tipo "Bar Chart"
* [x] **TASK-DVR-087**: Nomear: "Vendas por Região"
* [x] **TASK-DVR-088**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-089**: Configurar xAxis: coluna `regiao`
* [x] **TASK-DVR-090**: Configurar yAxis: agregação `SUM(valor_vendas)`
* [x] **TASK-DVR-091**: Configurar sort: by value, descending
* [x] **TASK-DVR-092**: Configurar formato: currency, BRL
* [x] **TASK-DVR-093**: Posicionar em (row:0, col:6, width:6, height:7)
* [x] **TASK-DVR-094**: Validar que mostra 4 regiões ordenadas por vendas
* [x] **TASK-DVR-095**: Salvar widget

### 5.4 Widget 3: Vendas por Mês

* [x] **TASK-DVR-096**: Adicionar widget tipo "Line Chart"
* [x] **TASK-DVR-097**: Nomear: "Vendas por Mês"
* [x] **TASK-DVR-098**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-099**: Configurar xAxis: coluna `mes_abrev`
* [x] **TASK-DVR-100**: ⚠️ **CRÍTICO**: NÃO configurar sort no widget (ordenação vem do ORDER BY no SQL!)
* [x] **TASK-DVR-101**: Configurar yAxis: agregação `SUM(valor_vendas)`
* [x] **TASK-DVR-102**: Configurar formato: currency, BRL
* [x] **TASK-DVR-103**: Posicionar em (row:7, col:0, width:6, height:7)
* [x] **TASK-DVR-104**: ⚠️ **VALIDAR CRÍTICO**: Eixo X mostra JAN→FEV→MAR→ABR→MAI (NÃO alfabético!)
* [x] **TASK-DVR-105**: Se ordenação estiver errada: voltar ao dataset, adicionar ORDER BY
* [x] **TASK-DVR-106**: Salvar widget

### 5.5 Widget 4: Desempenho da Categoria

* [x] **TASK-DVR-107**: Adicionar widget tipo "Bar Chart"
* [x] **TASK-DVR-108**: Nomear: "Desempenho da Categoria"
* [x] **TASK-DVR-109**: Conectar ao dataset `vendas_base_completa`
* [x] **TASK-DVR-110**: Configurar como horizontal bar (xAxis = valores, yAxis = categorias)
* [x] **TASK-DVR-111**: Configurar xAxis: agregação `SUM(valor_vendas)`
* [x] **TASK-DVR-112**: Configurar yAxis: coluna `secao`
* [x] **TASK-DVR-113**: Configurar sort: by value, descending
* [x] **TASK-DVR-114**: Configurar formato: currency, BRL
* [x] **TASK-DVR-115**: Posicionar em (row:7, col:6, width:6, height:7)
* [x] **TASK-DVR-116**: Validar que mostra 8 seções ordenadas por vendas
* [x] **TASK-DVR-117**: Salvar widget

---

## Fase 6: Aplicar Tema Visual

### 6.1 Configurar Tema

* [x] **TASK-DVR-118**: Navegar para Dashboard Settings
* [x] **TASK-DVR-119**: Clicar em "Theme"
* [x] **TASK-DVR-120**: Selecionar "Custom Theme"
* [x] **TASK-DVR-121**: Definir paleta de cores: ["#5B9BD5", "#70AD47", "#FFC000", "#ED7D31", "#A5A5A5", "#4472C4"]
* [x] **TASK-DVR-122**: Configurar canvas background (light: #E8EEF7, dark: #1A2332)
* [x] **TASK-DVR-123**: Configurar widget background (light: #FFFFFF, dark: #2D3E50)
* [x] **TASK-DVR-124**: Configurar fonte: Arial
* [x] **TASK-DVR-125**: Aplicar tema
* [x] **TASK-DVR-126**: Validar que widgets usam paleta azul profissional
* [x] **TASK-DVR-127**: Salvar configurações

---

## Fase 7: Testes e Validação

### 7.1 Teste de Baseline (Sem Filtros)

* [x] **TASK-DVR-128**: Limpar todos os filtros
* [x] **TASK-DVR-129**: Validar widget Vendedor: soma = R$ 7.004.651,22
* [x] **TASK-DVR-130**: Validar widget Região: soma = R$ 7.004.651,22
* [x] **TASK-DVR-131**: Validar widget Mês: soma = R$ 7.004.651,22
* [x] **TASK-DVR-132**: Validar widget Seção: soma = R$ 7.004.651,22
* [x] **TASK-DVR-133**: Documentar baseline: 1.000 transações, 8 vendedores, 4 regiões, 5 meses, 8 seções

### 7.2 Teste de Filtros Individuais

* [x] **TASK-DVR-134**: Aplicar filtro Região = Sul
* [x] **TASK-DVR-135**: Validar resultado: R$ 1.906.583,44 (250 tx)
* [x] **TASK-DVR-136**: Limpar filtro
* [x] **TASK-DVR-137**: Aplicar filtro Mês = MAR
* [x] **TASK-DVR-138**: Validar resultado: R$ 1.573.786,55 (226 tx)
* [x] **TASK-DVR-139**: Limpar filtro
* [x] **TASK-DVR-140**: Aplicar filtro Vendedor = Ricardo
* [x] **TASK-DVR-141**: Validar resultado: R$ 966.266,75 (131 tx)
* [x] **TASK-DVR-142**: Limpar filtro
* [x] **TASK-DVR-143**: Aplicar filtro Trimestre = Q1
* [x] **TASK-DVR-144**: Validar resultado: R$ 4.206.477,86 (558 tx)
* [x] **TASK-DVR-145**: Limpar filtro

### 7.3 Teste de Filtros Cruzados

* [x] **TASK-DVR-146**: Aplicar filtros combinados: Região=Sul + Mês=MAR
* [x] **TASK-DVR-147**: Validar resultado: R$ 448.572,29 (61 tx)
* [x] **TASK-DVR-148**: Validar que TODOS os 4 widgets mostram apenas Sul + MAR
* [x] **TASK-DVR-149**: Validar que gráfico de vendedor mostra apenas vendedores do Sul em Março
* [x] **TASK-DVR-150**: Validar que gráfico de região mostra apenas Sul
* [x] **TASK-DVR-151**: Validar que gráfico de mês mostra apenas MAR
* [x] **TASK-DVR-152**: Validar que gráfico de seção mostra apenas categorias vendidas no Sul em Março
* [x] **TASK-DVR-153**: Limpar filtros

### 7.4 Teste de Ordenação Cronológica

* [x] **TASK-DVR-154**: Visualizar widget "Vendas por Mês"
* [x] **TASK-DVR-155**: ⚠️ **CRÍTICO**: Validar que eixo X mostra: JAN → FEV → MAR → ABR → MAI
* [x] **TASK-DVR-156**: ❌ Se ordem estiver errada (ABR, FEV, JAN...): VOLTAR AO DATASET, ADICIONAR ORDER BY
* [x] **TASK-DVR-157**: Validar que linha do gráfico segue ordem cronológica

### 7.5 Teste de Performance

* [x] **TASK-DVR-158**: Medir tempo de carregamento do dashboard (< 5s)
* [x] **TASK-DVR-159**: Medir tempo de resposta de filtro (< 2s)
* [x] **TASK-DVR-160**: Medir tempo de refresh de widget (< 1s)
* [x] **TASK-DVR-161**: Documentar tempos medidos

### 7.6 Teste de Responsividade

* [x] **TASK-DVR-162**: Testar layout em resolução 1920x1080
* [x] **TASK-DVR-163**: Testar layout em resolução 1366x768
* [x] **TASK-DVR-164**: Validar que layout 2x2 é mantido
* [x] **TASK-DVR-165**: Validar que widgets não se sobrepõem

---

## Fase 8: Documentação e Versionamento

### 8.1 Exportar Definição

* [x] **TASK-DVR-166**: Usar `searchAssets` para buscar dashboard "Dashboard Vendas Regionais"
* [x] **TASK-DVR-167**: Usar `readAssetById` para ler definição completa (datasets, widgets, filtros)
* [x] **TASK-DVR-168**: Criar arquivo `dashboards/dashboard_vendas_regionais.json`
* [x] **TASK-DVR-169**: Salvar definição JSON no arquivo
* [x] **TASK-DVR-170**: Validar que JSON contém: metadados, datasets, filtros, widgets, tema

### 8.2 Documentar no SDD Local

* [x] **TASK-DVR-171**: Abrir `sdd_instructions.md`
* [x] **TASK-DVR-172**: Adicionar seção "Dashboards e Visualizações"
* [x] **TASK-DVR-173**: Documentar: ID, path, arquitetura, filtros, widgets
* [x] **TASK-DVR-174**: Documentar problemas resolvidos (ordenação, filtros cruzados)
* [x] **TASK-DVR-175**: Documentar baseline de dados
* [x] **TASK-DVR-176**: Documentar instruções para recriação
* [x] **TASK-DVR-177**: Atualizar versão do SDD (3.1 → 3.2)

### 8.3 Atualizar README

* [x] **TASK-DVR-178**: Abrir `README.md`
* [x] **TASK-DVR-179**: Adicionar dashboard como 4ª feature implementada
* [x] **TASK-DVR-180**: Adicionar seção "Dashboard Interativo"
* [x] **TASK-DVR-181**: Documentar: identificação, arquitetura, filtros, visualizações
* [x] **TASK-DVR-182**: Adicionar lições aprendidas
* [x] **TASK-DVR-183**: Adicionar baseline de dados
* [x] **TASK-DVR-184**: Atualizar estrutura de diretórios (incluir `dashboards/`)
* [x] **TASK-DVR-185**: Adicionar feature code DVR
* [x] **TASK-DVR-186**: Atualizar status do projeto
* [x] **TASK-DVR-187**: Atualizar versão do README (1.0 → 1.1)

### 8.4 Criar Documentação SDD

* [x] **TASK-DVR-188**: Criar diretório `sdd/features/dashboard_vendas_regionais/`
* [x] **TASK-DVR-189**: Criar `plan.md` (propósito, contexto, regras de negócio, estratégia)
* [x] **TASK-DVR-190**: Criar `spec.md` (arquitetura, datasets, SQL, filtros, widgets, testes)
* [x] **TASK-DVR-191**: Criar `tasks.md` (checklist granular - este arquivo)
* [x] **TASK-DVR-192**: Criar `TRACEABILITY_MATRIX.md` (rastreabilidade completa)
* [x] **TASK-DVR-193**: Validar que todos os arquivos seguem padrão SDD

---

## Fase 9: Validação Final

### 9.1 Checklist de Qualidade

* [x] **TASK-DVR-194**: Validar que dashboard renderiza sem erros
* [x] **TASK-DVR-195**: Validar que 5 filtros globais funcionam
* [x] **TASK-DVR-196**: Validar que 4 visualizações exibem dados corretos
* [x] **TASK-DVR-197**: Validar ordenação cronológica (JAN→MAI)
* [x] **TASK-DVR-198**: Validar valores monetários em formato BRL
* [x] **TASK-DVR-199**: Validar baseline de dados (R$ 7.004.651,22)
* [x] **TASK-DVR-200**: Validar que filtros cruzados funcionam
* [x] **TASK-DVR-201**: Validar performance (load < 5s, filtro < 2s)
* [x] **TASK-DVR-202**: Validar layout responsivo
* [x] **TASK-DVR-203**: Validar tema visual aplicado

### 9.2 Checklist de Documentação

* [x] **TASK-DVR-204**: Validar que `dashboards/dashboard_vendas_regionais.json` existe
* [x] **TASK-DVR-205**: Validar que `sdd_instructions.md` está atualizado
* [x] **TASK-DVR-206**: Validar que `README.md` está atualizado
* [x] **TASK-DVR-207**: Validar que `plan.md` está completo
* [x] **TASK-DVR-208**: Validar que `spec.md` tem TODOS os detalhes técnicos
* [x] **TASK-DVR-209**: Validar que `tasks.md` tem checklist completo
* [x] **TASK-DVR-210**: Validar que `TRACEABILITY_MATRIX.md` está completo

### 9.3 Validar Reprodutibilidade

* [x] **TASK-DVR-211**: Ler `spec.md` e validar que OUTRO AGENTE conseguiria reproduzir dashboard
* [x] **TASK-DVR-212**: Validar que SQL está completo no spec.md
* [x] **TASK-DVR-213**: Validar que configurações de widgets estão detalhadas
* [x] **TASK-DVR-214**: Validar que posições de widgets estão documentadas
* [x] **TASK-DVR-215**: Validar que problemas resolvidos estão documentados

---

## Observações Importantes

### ⚠️ Regras CRÍTICAS

1. **ORDER BY no Dataset**: NUNCA remover `ORDER BY ano, mes` do SQL. Sem ele, ordenação cronológica quebra.
2. **Dataset Único**: TODOS os widgets devem usar `vendas_base_completa`. Criar datasets separados quebra filtros cruzados.
3. **NÃO Ordenar no Widget**: Sort customizado no widget de linha NÃO funciona. Ordenação DEVE vir do SQL.
4. **Validar Baseline**: Soma total DEVE ser R$ 7.004.651,22. Divergência indica erro de configuração.

### 📝 Problemas Resolvidos Documentados

1. **Ordenação de Meses**: Resolvido com `ORDER BY` no SQL (tasks 21, 100, 104, 105, 154-157)
2. **Filtros Cruzados**: Resolvido com dataset único (tasks 77, 88, 98, 109)
3. **Categorias Desordenadas**: Resolvido com `sort: descending` no widget (tasks 80, 91, 113)

### ✅ Validação Completa

* **Total de Tasks**: 215
* **Tasks Concluídas**: 215 (✅ 100%)
* **Tasks Falhadas**: 0
* **Taxa de Sucesso**: 100%

---

**Última Atualização**: 2026-04-19  
**Status**: ✅ 100% Completo  
**Próxima Revisão**: 2026-07-19 (trimestral)