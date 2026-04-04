# Plan: Synthetic Data Generator

**Feature Code**: SDG  
**Feature Name**: synthetic_data_generator  
**Versão**: 2.0  
**Data de Criação**: 2026-04-04  
**Última Atualização**: 2026-04-04  
**Autor**: Data In Code Team

---

## Propósito

Criar uma solução **100% independente** para **geração de dados sintéticos** de vendas regionais. A feature gera conjuntos de dados de teste com volume configurável, preservando as características estatísticas e relações extraídas de dados reais (90 registros, jan-mai/2018).

**Diferencial v2.0**: Metadata estatístico **pré-configurado no código**, eliminando qualquer dependência de arquivos externos (Excel, CSV, etc.).

Esta feature é essencial para:
- **Testes de desenvolvimento**: criar ambientes de teste sem expor dados reais ou depender de arquivos
- **Validação de pipelines**: testar processamento com volumes variados
- **Demonstrações**: apresentar soluções sem comprometer dados sensíveis
- **Experimentação**: explorar cenários com diferentes volumes de dados
- **Portabilidade**: executar em qualquer ambiente sem setup de arquivos

---

## Contexto de Negócio

### Problema

O projeto vendas_regionais trabalha com dados reais de vendas que contêm informações sensíveis. Para desenvolvimento, testes e demonstrações, precisamos de dados que:
1. Mantenham a **estrutura e formato** dos dados originais
2. Preservem **distribuições estatísticas** realistas
3. Permitam **controle de volume** (quantidade de registros)
4. Sejam **gerados rapidamente** sob demanda
5. **NÃO dependam de arquivos externos** (total portabilidade)

### Metadata Fonte (Pré-configurado)

Os dados sintéticos são baseados em metadata extraído de dados reais:

**Fonte Original**: Dados de vendas regionais (90 registros)  
**Período**: Janeiro a Maio de 2018  
**Status**: Metadata pré-configurado no código (v2.0)

**Estrutura dos Dados**:
- Data da Venda (datetime): 2018-01-03 a 2018-05-31
- Região (4 valores): Norte (23,3%), Sul (26,7%), Sudeste (25,6%), Nordeste (24,4%)
- Vendedor (8 vendedores): Roberto, Ricardo, Rodrigo, Roberta, Renata, Rafael, Raquel, Ronaldo
- Código Vendedor (1-8): Mapeamento 1:1 com vendedor
- Seção (8 categorias): Eletrônicos, Eletrodomésticos, Móveis, Informática, Telefonia, Games, Livros, Automotivo
- Vendas (valores monetários): R$ 366,34 a R$ 19.228,10 (distribuição log-normal)
- Mês (JAN, FEV, MAR, ABR, MAI): Derivado automaticamente da data

---

## Regras de Negócio

### Regra 1: Preservação de Distribuições Estatísticas

**Descrição**: Os dados sintéticos devem manter as distribuições pré-configuradas de cada coluna.

**Implementação**:
- **Região**: Manter proporções aproximadas dos 4 valores (distribuição pré-configurada)
- **Vendedor**: Distribuir uniformemente entre os 8 vendedores
- **Seção**: Manter proporções das 8 categorias (distribuição pré-configurada)
- **Vendas**: Gerar valores dentro do intervalo R$ 366-19.228 com distribuição log-normal
- **Data da Venda**: Gerar datas dentro do período 2018-01-03 a 2018-05-31

**Exemplo**: Se nos dados originais a região "Norte" representa 23,3% dos registros, a região Norte nos dados sintéticos deve representar aproximadamente 23,3%.

### Regra 2: Consistência de Relacionamentos

**Descrição**: Manter a integridade referencial entre colunas relacionadas.

**Implementação**:
- **Código Vendedor ↔ Vendedor**: Mapeamento 1:1 pré-configurado (código 1 = Roberto, código 2 = Ricardo, etc.)
- **Mês ↔ Data da Venda**: O mês deve corresponder à data gerada (ex: ABR para abril)

**Exceções**: Nenhuma. Todos os registros devem manter consistência.

### Regra 3: Volume Configurável

**Descrição**: O usuário deve poder especificar quantos registros deseja gerar.

**Implementação**:
- Parâmetro de entrada: `n_registros` (inteiro positivo)
- Sem limite superior definido (performance permitindo)
- Valor padrão sugerido: 1000 registros

**Exemplo**: Executar com `n_registros=5000` deve gerar exatamente 5000 linhas de dados sintéticos.

### Regra 4: Aleatoriedade Reproduzível

**Descrição**: Permitir reprodução dos mesmos dados sintéticos quando necessário.

**Implementação**:
- Parâmetro opcional: `random_seed` (inteiro)
- Se fornecido: mesmos dados são gerados
- Se omitido: dados diferentes a cada execução

**Exemplo**: Executar duas vezes com `random_seed=42` deve gerar os mesmos registros.

---

## Estratégia de Implementação

### Abordagem Técnica

**Tecnologia Escolhida**: Python + Pandas + NumPy

**Justificativa**:
1. **Pandas**: Excelente para manipulação de DataFrames
2. **NumPy**: Geração eficiente de números aleatórios com distribuições estatísticas
3. **Simplicidade**: Dados em volume moderado não requerem Spark
4. **Portabilidade**: Pode ser executado em qualquer ambiente Python
5. **Independência**: Não requer arquivos externos (v2.0)

### Estratégia de Geração

**Fase 1: Carregamento de Metadata Pré-configurado**
1. Carregar distribuições categóricas (Região, Seção) do código
2. Carregar lista de vendedores e mapeamento código-vendedor
3. Carregar parâmetros da distribuição log-normal para vendas
4. Carregar intervalo de datas

**Fase 2: Geração Sintética**
1. Gerar datas aleatórias dentro do período 2018-01-03 a 2018-05-31
2. Derivar mês a partir da data gerada
3. Gerar regiões usando distribuição pré-configurada
4. Gerar vendedores usando distribuição uniforme
5. Mapear código_vendedor a partir do vendedor (mapeamento 1:1)
6. Gerar seções usando distribuição pré-configurada
7. Gerar valores de vendas usando distribuição log-normal

**Fase 3: Validação e Export**
1. Validar integridade (sem nulos, tipos corretos)
2. Validar relacionamentos (código ↔ vendedor, mês ↔ data)
3. Exportar para formato desejado (Excel, CSV, ou Delta table)

### Justificativa de Escolhas

**Por que não Spark?**
- Volume de dados pequeno/moderado (< 1M registros típico)
- Pandas é mais simples e direto para este caso
- Overhead de Spark seria desnecessário

**Por que Log-Normal para Vendas?**
- Valores de vendas tendem a seguir distribuição log-normal (muitos valores baixos, poucos valores altos)
- Melhor representa realidade do que distribuição uniforme

**Por que Metadata Pré-configurado (v2.0)?**
- **Independência total**: Não requer setup de arquivos
- **Portabilidade**: Executa em qualquer ambiente
- **Simplicidade**: Zero dependências externas de dados
- **Performance**: Sem overhead de leitura de arquivos

---

## Dependências

### Internas
- **Feature**: error_handler_logging (EHL)
  - LogControl para logging padronizado
  - Path: `/data-in-code/error_handler_logging/src/logger_control`

### Externas
- **Biblioteca**: pandas (manipulação de DataFrames)
- **Biblioteca**: numpy (geração de números aleatórios)
- **Biblioteca**: openpyxl (suporte a export para .xlsx, se necessário)

### Dados
- **Nenhuma dependência de arquivos externos** (v2.0)
- Metadata estatístico pré-configurado no código

---

## Métricas de Sucesso

### Critérios de Aceitação

1. **Funcionalidade Básica**
   - ✅ Gera dados sintéticos com volume configurável
   - ✅ Mantém estrutura idêntica aos dados originais (mesmas colunas e tipos)
   - ✅ Executa em menos de 10 segundos para 10.000 registros
   - ✅ **NÃO requer nenhum arquivo externo** (v2.0)

2. **Qualidade dos Dados**
   - ✅ Distribuição de regiões aproxima-se do metadata pré-configurado (±10%)
   - ✅ Distribuição de seções aproxima-se do metadata pré-configurado (±10%)
   - ✅ Valores de vendas dentro do intervalo observado
   - ✅ 100% de consistência código_vendedor ↔ vendedor
   - ✅ 100% de consistência mês ↔ data_venda

3. **Usabilidade**
   - ✅ Interface clara com parâmetros bem documentados
   - ✅ Mensagens de log informativos durante execução
   - ✅ Export para múltiplos formatos (Excel, CSV, Delta)

4. **Conformidade SDD**
   - ✅ Logging via LogControl integrado
   - ✅ Tratamento de erros com error_handler
   - ✅ Documentação completa (plan/spec/tasks/matriz)
   - ✅ Testes automatizados implementados

### Como Validar

**Teste 1: Volume**
```python
n_registros = 1000
df_synth = gerar_dados_sinteticos(n_registros)
assert len(df_synth) == 1000
```

**Teste 2: Distribuição de Regiões**
```python
# Comparar proporções pré-configuradas vs sintéticas
config_props = metadata['regiao_dist']  # Proporções pré-configuradas
synth_props = df_synth['Região'].value_counts(normalize=True)
assert (abs(config_props - synth_props) < 0.1).all()  # ±10%
```

**Teste 3: Consistência Código-Vendedor**
```python
# Todo código 1 deve ser Roberto, código 2 Ricardo, etc.
for vendedor, codigo in df_synth[['Vendedor', 'Código Vendedor']].drop_duplicates().values:
    assert metadata['codigo_vendedor_map'][vendedor] == codigo
```

**Teste 4: Independência (v2.0)**
```python
# Executar sem nenhum arquivo externo disponível
import os
assert not os.path.exists('VendasRegionaisVBA.xlsm')  # Garantir que arquivo não existe
df_synth = gerar_dados_sinteticos(1000)  # Deve funcionar normalmente
assert len(df_synth) == 1000
```

---

## Riscos e Mitigações

| Risco | Probabilidade | Impacto | Mitigação |
|-------|---------------|---------|----------|
| Dados sintéticos não realistas | Baixa (v2.0) | Alto | Metadata extraído de dados reais e validado estatisticamente |
| Performance ruim com volumes grandes (>100k) | Baixa | Médio | Documentar limites recomendados, otimizar se necessário |
| ~~Arquivo Excel original alterado~~ | N/A (v2.0) | N/A | Metadata pré-configurado, sem dependência externa |
| Falta de diversidade em dados gerados | Média | Baixo | Usar múltiplas distribuições estatísticas e aleatoriedade |
| Metadata desatualizado | Baixa | Baixo | Documentar origem do metadata (90 registros, jan-mai/2018) |

---

## Próximos Passos

1. ✅ Criar `spec.md` com detalhamento técnico (atualizado v2.0)
2. ✅ Criar `tasks.md` com checklist de implementação (atualizado v2.0)
3. ✅ Implementar notebook `nb_synthetic_data_generator` (v2.0 - independente)
4. ✅ Criar notebook de testes `nb_test_synthetic_data_generator`
5. ✅ Validar com diferentes volumes (100, 1.000, 10.000 registros)
6. ✅ Validar independência total (sem arquivos externos)
7. ✅ Documentar exemplos de uso
8. ✅ Integrar com pipeline principal se aplicável

---

## Histórico de Versões

| Versão | Data | Mudanças |
|--------|------|----------|
| 1.0 | 2026-04-04 | Versão inicial com dependência de arquivo Excel |
| 2.0 | 2026-04-04 | **Metadata pré-configurado** - 100% independente de arquivos externos |

---

**Status**: ✅ Planejamento Completo (v2.0)  
**Próximo Documento**: `spec.md` (Especificação Técnica)