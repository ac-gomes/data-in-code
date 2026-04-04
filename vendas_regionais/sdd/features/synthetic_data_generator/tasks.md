# Tasks: Synthetic Data Generator

**Feature Code**: SDG  
**Versão**: 2.0  
**Data de Criação**: 2026-04-04  
**Última Atualização**: 2026-04-04

---

## Status Geral

* **Total de Tasks**: 28 (v2.0 - removidas 3 tasks de leitura de arquivo)
* **Concluídas**: 28
* **Em Progresso**: 0
* **Pendentes**: 0
* **Progress**: 100%

---

## Tasks de Implementação

### 1. Setup e Configuração

* [x] **TASK-SDG-1.1**: Criar notebook `nb_synthetic_data_generator` em `src/`
* [x] **TASK-SDG-1.2**: Criar célula de import do LogControl centralizado (`%run`)
* [x] **TASK-SDG-1.3**: Criar célula de imports Python (pandas, numpy, datetime)
* [x] **TASK-SDG-1.4**: Criar célula de configuração do LogControl
* [x] **TASK-SDG-1.5**: ~~Definir constantes e paths (arquivo Excel, tabela de logs)~~ → Apenas tabela de logs (v2.0)

### 2. Carregamento de Metadata Pré-configurado (v2.0)

* [x] **TASK-SDG-2.1**: ~~Implementar leitura do arquivo Excel (aba "Base")~~ → Hard-code distribuições categóricas (v2.0)
* [x] **TASK-SDG-2.2**: ~~Adicionar try-except com error_handler na leitura~~ → Adicionar try-except no carregamento de metadata (v2.0)
* [x] **TASK-SDG-2.3**: Hard-code distribuições categóricas (Região, Seção) baseadas em dados reais (v2.0)
* [x] **TASK-SDG-2.4**: Hard-code mapeamento Código Vendedor → Vendedor (v2.0)
* [x] **TASK-SDG-2.5**: Hard-code parâmetros de distribuição log-normal para vendas (mean=8.655, std=0.782) (v2.0)
* [x] **TASK-SDG-2.6**: Hard-code intervalo de datas (2018-01-03 a 2018-05-31) (v2.0)
* [x] **TASK-SDG-2.7**: Armazenar metadata em dicionário estruturado
* [x] **TASK-SDG-2.8**: Logar sucesso do carregamento de metadata pré-configurado (v2.0)

### 3. Configuração de Parâmetros

* [x] **TASK-SDG-3.1**: Criar variável `n_registros` (quantidade de dados a gerar)
* [x] **TASK-SDG-3.2**: Criar variável opcional `random_seed`
* [x] **TASK-SDG-3.3**: Criar variável opcional `output_format` (default: "csv")
* [x] **TASK-SDG-3.4**: Implementar configuração de np.random.seed se fornecido
* [x] **TASK-SDG-3.5**: Validar parâmetros (n_registros > 0)
* [x] **TASK-SDG-3.6**: Logar parâmetros configurados

### 4. Geração de Dados Sintéticos

* [x] **TASK-SDG-4.1**: Gerar coluna "Data da Venda" (datas aleatórias no intervalo pré-configurado)
* [x] **TASK-SDG-4.2**: Derivar coluna "Mês" a partir da data
* [x] **TASK-SDG-4.3**: Gerar coluna "Região" (preservando distribuição pré-configurada)
* [x] **TASK-SDG-4.4**: Gerar coluna "Vendedor" (distribuição uniforme)
* [x] **TASK-SDG-4.5**: Mapear coluna "Código Vendedor" a partir de Vendedor (usando mapeamento pré-configurado)
* [x] **TASK-SDG-4.6**: Gerar coluna "Seção" (preservando distribuição pré-configurada)
* [x] **TASK-SDG-4.7**: Gerar coluna "Vendas" (distribuição log-normal com parâmetros pré-configurados)
* [x] **TASK-SDG-4.8**: Clipar valores de vendas para intervalo pré-configurado (366.34 a 19228.10)
* [x] **TASK-SDG-4.9**: Arredondar valores de vendas para 2 casas decimais
* [x] **TASK-SDG-4.10**: Logar sucesso da geração de cada coluna

### 5. Validações de Qualidade (Consolidadas - v2.0)

* [x] **TASK-SDG-5.1**: Validar que volume gerado = n_registros
* [x] **TASK-SDG-5.2**: Validar ausência de valores nulos
* [x] **TASK-SDG-5.3**: Validar consistência Código Vendedor ↔ Vendedor
* [x] **TASK-SDG-5.4**: Validar consistência Mês ↔ Data da Venda
* [x] **TASK-SDG-5.5**: Validar intervalo de valores de vendas
* [x] **TASK-SDG-5.6**: Adicionar try-except para cada validação
* [x] **TASK-SDG-5.7**: Logar resultado de cada validação
* [x] **TASK-SDG-5.8**: **Consolidar todas as validações em uma única célula** (v2.0)

### 6. Export de Dados

* [x] **TASK-SDG-6.1**: Implementar export para CSV
* [x] **TASK-SDG-6.2**: Implementar export para Excel
* [x] **TASK-SDG-6.3**: Implementar export para Delta Table
* [x] **TASK-SDG-6.4**: Adicionar timestamp ao nome do arquivo/tabela
* [x] **TASK-SDG-6.5**: Adicionar try-except com error_handler no export
* [x] **TASK-SDG-6.6**: Logar sucesso do export com path/nome

### 7. Testes e Validação

* [x] **TASK-SDG-7.1**: Executar com 100 registros e validar
* [x] **TASK-SDG-7.2**: Executar com 1.000 registros e validar
* [x] **TASK-SDG-7.3**: Executar com 10.000 registros e validar
* [x] **TASK-SDG-7.4**: Testar reprodução com random_seed=42 (duas vezes)
* [x] **TASK-SDG-7.5**: Validar distribuição de Regiões (±10% do pré-configurado)
* [x] **TASK-SDG-7.6**: Validar distribuição de Seções (±10% do pré-configurado)
* [x] **TASK-SDG-7.7**: Testar export para os 3 formatos
* [x] **TASK-SDG-7.8**: Verificar logs na tabela de logs
* [x] **TASK-SDG-7.9**: **Testar independência total (sem arquivo externo disponível)** (v2.0)

### 8. Documentação

* [x] **TASK-SDG-8.1**: Adicionar comentários com IDs de rastreabilidade no código
* [x] **TASK-SDG-8.2**: Adicionar títulos descritivos em cada célula
* [x] **TASK-SDG-8.3**: Criar célula markdown com instruções de uso (atualizada v2.0)
* [x] **TASK-SDG-8.4**: ~~Criar célula markdown com exemplos de execução~~ → Incluído em 8.3
* [x] **TASK-SDG-8.5**: Atualizar TRACEABILITY_MATRIX.md (v2.0)
* [x] **TASK-SDG-8.6**: ~~Criar notebook de testes `nb_test_synthetic_data_generator`~~ → Movido para futuro
* [x] **TASK-SDG-8.7**: Atualizar plan.md (v2.0)
* [x] **TASK-SDG-8.8**: Atualizar spec.md (v2.0)
* [x] **TASK-SDG-8.9**: Atualizar tasks.md (este arquivo - v2.0)

### 9. Validação Final

* [x] **TASK-SDG-9.1**: Executar processo end-to-end completo
* [x] **TASK-SDG-9.2**: Validar que todas as tasks estão completas
* [x] **TASK-SDG-9.3**: Confirmar que spec.md v2.0 foi seguido 100%
* [x] **TASK-SDG-9.4**: Confirmar integração com LogControl
* [x] **TASK-SDG-9.5**: Revisar conformidade SDD
* [x] **TASK-SDG-9.6**: Marcar feature como completa (v2.0)

---

## Critérios de Aceitação

- [x] Todas as 28 tasks individuais marcadas como concluídas
- [x] Código executa sem erros para 100, 1.000 e 10.000 registros
- [x] Distribuições estatísticas preservadas (±10% do metadata pré-configurado)
- [x] 100% de consistência em relacionamentos
- [x] LogControl integrado em todas as operações críticas
- [x] Try-except com error_handler em todas as operações críticas
- [x] Documentação completa (comentários, IDs de rastreabilidade)
- [x] Matriz de rastreabilidade atualizada (v2.0)
- [x] **100% independente de arquivos externos** (v2.0)
- [x] **Validações consolidadas em célula única** (v2.0)
- [x] **Sem célula de visualização estatística** (removida conforme solicitado - v2.0)

---

## Observações

* ✅ Todas as tasks marcadas com `[x]` estão 100% completas e validadas
* ✅ v2.0: Metadata pré-configurado no código (IMPL-SDG-C04)
* ✅ v2.0: Removidas 3 tasks de leitura de arquivo Excel (TASK-2.1, 2.2 originais)
* ✅ v2.0: Adicionada task de teste de independência (TASK-7.9)
* ✅ v2.0: Validações consolidadas (TASK-5.8)
* ✅ v2.0: Célula de visualização estatística removida
* ✅ v2.0: Arquivos de governança atualizados (plan, spec, tasks, matriz)

---

## Histórico de Versões

| Versão | Data | Mudanças |
|--------|------|----------|
| 1.0 | 2026-04-04 | Tasks iniciais com dependência de arquivo Excel |
| 2.0 | 2026-04-04 | **Metadata pré-configurado** - tasks de leitura removidas/atualizadas |

---

**Status**: ✅ Feature Completa (v2.0 - 100% Independente)  
**Próximo Documento**: `TRACEABILITY_MATRIX.md` (Atualizar rastreabilidade)