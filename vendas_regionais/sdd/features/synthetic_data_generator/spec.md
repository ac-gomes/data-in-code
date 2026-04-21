# Especificação Técnica: Synthetic Data Generator

**Feature Code**: SDG  
**Versão**: 2.0  
**Data**: 2026-04-04  
**Status**: ✅ 100% Independente

---

## Visão Geral

Feature para geração automatizada de dados sintéticos **100% independente de arquivos externos**, com metadata estatístico pré-configurado no código. Preserva distribuições estatísticas e relacionamentos entre variáveis baseados em dados reais de vendas regionais (90 registros, jan-mai/2018). Implementado em Python/Pandas com suporte a volume configurável e reprodução determinística.

---

## Requisitos Funcionais

### SPEC-SDG-R01: Metadata Pré-configurado (v2.0)
**Descrição**: Carregar metadata estatístico pré-configurado no código (distribuições, intervalos, mapeamentos).  
**Prioridade**: Alta  
**Input**: Nenhum (metadata hard-coded)  
**Output**: Dicionário `metadata` com distribuições, intervalos e mapeamentos  
**Mudança v2.0**: Anterior "Leitura de Dados Originais" de arquivo Excel. Agora metadata pré-configurado.

**Metadata Incluído**:
- Distribuições categóricas: Região (4 valores), Seção (8 valores)
- Lista de vendedores (8 vendedores)
- Mapeamento Código Vendedor → Vendedor (1:1)
- Parâmetros distribuição log-normal para vendas (mean, std)
- Intervalo de vendas: R$ 366,34 a R$ 19.228,10
- Intervalo de datas: 2018-01-03 a 2018-05-31

### SPEC-SDG-R02: Geração de Volume Configurável
**Descrição**: Gerar N registros sintéticos conforme parâmetro do usuário.  
**Prioridade**: Alta  
**Parâmetro**: `n_registros` (int, obrigatório)  
**Output**: DataFrame com exatamente N registros

### SPEC-SDG-R03: Preservação de Distribuições Categóricas
**Descrição**: Manter proporções de valores categóricos (Região, Seção) do metadata pré-configurado.  
**Prioridade**: Alta  
**Tolerância**: ±10% das proporções pré-configuradas  
**Colunas Afetadas**: `Região`, `Seção`

### SPEC-SDG-R04: Geração de Valores Numéricos Realistas
**Descrição**: Gerar valores de vendas seguindo distribuição log-normal com parâmetros pré-configurados.  
**Prioridade**: Alta  
**Intervalo**: R$ 366,34 a R$ 19.228,10  
**Distribuição**: Log-normal(mu=8.655, sigma=0.782)

### SPEC-SDG-R05: Consistência de Relacionamentos
**Descrição**: Garantir integridade referencial entre colunas relacionadas.  
**Prioridade**: Alta  
**Relações**:
- `Código Vendedor` → `Vendedor` (mapeamento 1:1 pré-configurado)
- `Data da Venda` → `Mês` (derivação automática)

### SPEC-SDG-R06: Reprodução Determinística
**Descrição**: Permitir reprodução dos mesmos dados via seed.  
**Prioridade**: Média  
**Parâmetro**: `random_seed` (int, opcional)  
**Comportamento**: Se fornecido, gerar mesmos dados; se omitido, aleatoriedade total

### SPEC-SDG-R07: Export Multi-Formato
**Descrição**: Exportar dados gerados para múltiplos formatos.  
**Prioridade**: Média  
**Formatos Suportados**: Excel (.xlsx), CSV (.csv), Delta Table  
**Parâmetro**: `output_format` (string, opcional, default="csv")

### SPEC-SDG-R08: Logging Padronizado
**Descrição**: Integrar LogControl para rastreabilidade.  
**Prioridade**: Alta  
**Implementação**: Usar LogControl centralizado (`/data-in-code/error_handler_logging/`)  
**Eventos a Logar**:
- Início de execução com parâmetros
- Carregamento de metadata pré-configurado (v2.0)
- Geração de cada coluna
- Validações realizadas
- Export de dados
- Erros e exceções

---

## Arquitetura de Dados

### Input (v2.0)

**Source**: Metadata pré-configurado no código  
**Origem**: Extraído de dados reais (90 registros, jan-mai/2018)  
**Localização**: IMPL-SDG-C04 (célula do notebook)  
**Dependências Externas**: Nenhuma ✅

**Metadata Pré-configurado**:

| Atributo | Tipo | Descrição | Exemplo |
|----------|------|-------------|----------|
| regiao_dist | dict | Distribuição de regiões (4 valores) | {'Norte': 0.2333, 'Sul': 0.2667, ...} |
| secao_dist | dict | Distribuição de seções (8 valores) | {'Eletrônicos': 0.1556, ...} |
| vendedores | list | Lista de 8 vendedores | ['Roberto', 'Ricardo', ...] |
| codigo_vendedor_map | dict | Mapeamento vendedor → código | {'Roberto': 1, 'Ricardo': 2, ...} |
| vendas_log_mean | float | Média log(vendas) | 8.655 |
| vendas_log_std | float | Desvio padrão log(vendas) | 0.782 |
| vendas_min | float | Valor mínimo de vendas | 366.34 |
| vendas_max | float | Valor máximo de vendas | 19228.10 |
| data_min | Timestamp | Data inicial | 2018-01-03 |
| data_max | Timestamp | Data final | 2018-05-31 |

### Output

**Destination**: Configurável (CSV, Excel, ou Delta)  
**Default**: CSV no mesmo diretório do notebook  
**Schema Sintético**: Idêntico aos dados originais  
**Volume**: Configurável via parâmetro

**Schema Output**:

| Coluna | Tipo | Exemplo | Origem |
|--------|------|---------|--------|
| Data da Venda | datetime64[ns] | 2018-04-23 | Gerado aleatoriamente no intervalo pré-configurado |
| Região | object | "Norte" | Gerado com distribuição pré-configurada |
| Vendedor | object | "Roberto" | Gerado uniformemente dos 8 vendedores |
| Código Vendedor | int64 | 1 | Mapeado via metadata pré-configurado |
| Seção | object | "Eletrônicos" | Gerado com distribuição pré-configurada |
| Vendas | float64 | 1347.68 | Gerado com distribuição log-normal |
| Mês | object | "ABR" | Derivado automaticamente da data |

---

## Fluxo de Processamento

### 1. Inicialização

**Objetivo**: Configurar ambiente e parâmetros

```python
# IMPL-SDG-C01: Import do LogControl centralizado
%run /Workspace/Users/data.in.code/data-in-code/error_handler_logging/src/logger_control

# IMPL-SDG-C02: Imports necessários
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# IMPL-SDG-C03: Configuração do logger
logger = LogControl(
    logger_name="synthetic_data_generator",
    tbl_name="main.vendas_regionais.tb_logs_sdg"
)

logger.log_info("Inicializando Synthetic Data Generator v2.0")
```

**Validações**:
- Verificar parâmetros obrigatórios
- Validar `n_registros > 0`

---

### 2. Carregamento de Metadata Pré-configurado (v2.0)

**Objetivo**: Carregar metadata estatístico do código

```python
# IMPL-SDG-C04: Metadata pré-configurado
try:
    # Distribuições categóricas
    regiao_dist = {
        'Norte': 0.2333, 'Sul': 0.2667,
        'Sudeste': 0.2556, 'Nordeste': 0.2444
    }
    
    secao_dist = {
        'Eletrônicos': 0.1556,
        'Eletrodomésticos': 0.1111,
        # ... outras seções
    }
    
    # Vendedores e mapeamentos
    vendedores = ['Roberto', 'Ricardo', 'Rodrigo', 'Roberta',
                  'Renata', 'Rafael', 'Raquel', 'Ronaldo']
    
    codigo_vendedor_map = {
        'Roberto': 1, 'Ricardo': 2, 'Rodrigo': 3, 'Roberta': 4,
        'Renata': 5, 'Rafael': 6, 'Raquel': 7, 'Ronaldo': 8
    }
    
    # Parâmetros de vendas
    vendas_log_mean = 8.655
    vendas_log_std = 0.782
    vendas_min = 366.34
    vendas_max = 19228.10
    
    # Intervalo de datas
    data_min = pd.Timestamp('2018-01-03')
    data_max = pd.Timestamp('2018-05-31')
    
    # Armazenar metadata
    metadata = {
        'regiao_dist': regiao_dist,
        'secao_dist': secao_dist,
        'vendedores': vendedores,
        'codigo_vendedor_map': codigo_vendedor_map,
        'vendas_log_mean': vendas_log_mean,
        'vendas_log_std': vendas_log_std,
        'vendas_min': vendas_min,
        'vendas_max': vendas_max,
        'data_min': data_min,
        'data_max': data_max
    }
    
    logger.log_success(f"Metadata pré-configurado carregado: {len(metadata)} atributos")
    
except Exception as e:
    logger.log_error("Erro ao carregar metadata")
    logger.error_handler(e, debug_write_mode=False)
    raise
```

**Output**: Dicionário `metadata` com todas as distribuições e parâmetros

---

### 3. Configuração de Aleatoriedade

**Objetivo**: Configurar seed se fornecido

```python
# IMPL-SDG-C06: Configuração de random seed
if random_seed is not None:
    np.random.seed(random_seed)
    logger.log_info(f"Random seed configurado: {random_seed}")
else:
    logger.log_info("Random seed não configurado (aleatoriedade total)")
```

---

### 4. Geração de Dados Sintéticos

**Objetivo**: Criar DataFrame com N registros sintéticos

```python
# IMPL-SDG-C07: Inicializar DataFrame vazio
df_synth = pd.DataFrame()

# IMPL-SDG-C08: Gerar datas aleatórias
date_range_days = (metadata['data_max'] - metadata['data_min']).days
random_days = np.random.randint(0, date_range_days + 1, size=n_registros)
df_synth['Data da Venda'] = metadata['data_min'] + pd.to_timedelta(random_days, unit='D')
logger.log_success(f"Coluna 'Data da Venda' gerada ({n_registros} registros)")

# IMPL-SDG-C09: Derivar mês da data
mes_map = {1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
           7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'}
df_synth['Mês'] = df_synth['Data da Venda'].dt.month.map(mes_map)
logger.log_success("Coluna 'Mês' derivada automaticamente")

# IMPL-SDG-C10: Gerar regiões com distribuição pré-configurada
regioes = list(metadata['regiao_dist'].keys())
probs = list(metadata['regiao_dist'].values())
df_synth['Região'] = np.random.choice(regioes, size=n_registros, p=probs)
logger.log_success("Coluna 'Região' gerada (distribuição preservada)")

# IMPL-SDG-C11: Gerar vendedores uniformemente
df_synth['Vendedor'] = np.random.choice(metadata['vendedores'], size=n_registros)
logger.log_success("Coluna 'Vendedor' gerada")

# IMPL-SDG-C12: Mapear código vendedor
df_synth['Código Vendedor'] = df_synth['Vendedor'].map(metadata['codigo_vendedor_map'])
logger.log_success("Coluna 'Código Vendedor' mapeada")

# IMPL-SDG-C13: Gerar seções com distribuição pré-configurada
secoes = list(metadata['secao_dist'].keys())
probs_secao = list(metadata['secao_dist'].values())
df_synth['Seção'] = np.random.choice(secoes, size=n_registros, p=probs_secao)
logger.log_success("Coluna 'Seção' gerada (distribuição preservada)")

# IMPL-SDG-C14: Gerar valores de vendas (log-normal)
vendas_synth = np.random.lognormal(
    mean=metadata['vendas_log_mean'],
    sigma=metadata['vendas_log_std'],
    size=n_registros
)
# Clipar para intervalo observado
vendas_synth = np.clip(vendas_synth, metadata['vendas_min'], metadata['vendas_max'])
df_synth['Vendas'] = np.round(vendas_synth, 2)
logger.log_success("Coluna 'Vendas' gerada (distribuição log-normal)")
```

**Output**: DataFrame `df_synth` com todas as colunas geradas

---

### 5. Validações de Qualidade

**Objetivo**: Garantir integridade dos dados sintéticos

```python
# IMPL-SDG-C15: Validações consolidadas
try:
    logger.log_info("=" * 80)
    logger.log_info("Executando validações de qualidade")
    logger.log_info("=" * 80)
    
    # Validação 1: Volume
    assert len(df_synth) == n_registros, "Volume incorreto"
    logger.log_success(f"✅ Validação 1: Volume correto ({n_registros} registros)")
    
    # Validação 2: Sem nulos
    assert df_synth.isnull().sum().sum() == 0, "Dados contêm valores nulos"
    logger.log_success("✅ Validação 2: Sem valores nulos")
    
    # Validação 3: Consistência código-vendedor
    codigo_check = df_synth.groupby('Vendedor')['Código Vendedor'].nunique()
    assert (codigo_check == 1).all(), "Inconsistência código-vendedor"
    logger.log_success("✅ Validação 3: Consistência código-vendedor OK")
    
    # Validação 4: Consistência mês-data
    df_synth['_mes_calc'] = df_synth['Data da Venda'].dt.month.map(mes_map)
    assert (df_synth['Mês'] == df_synth['_mes_calc']).all(), "Inconsistência mês-data"
    df_synth.drop(columns=['_mes_calc'], inplace=True)
    logger.log_success("✅ Validação 4: Consistência mês-data OK")
    
    # Validação 5: Intervalo de vendas
    assert df_synth['Vendas'].min() >= metadata['vendas_min'], "Vendas abaixo do mínimo"
    assert df_synth['Vendas'].max() <= metadata['vendas_max'], "Vendas acima do máximo"
    logger.log_success(f"✅ Validação 5: Intervalo de vendas OK")
    
    logger.log_info("=" * 80)
    logger.log_success("✅ Todas as validações passaram!")
    logger.log_info("=" * 80)
    
except AssertionError as e:
    logger.log_error(f"Validação falhou: {str(e)}")
    logger.error_handler(e, debug_write_mode=False)
    raise
```

---

### 6. Export de Dados

**Objetivo**: Salvar dados sintéticos no formato escolhido

```python
# IMPL-SDG-C16: Export conforme formato escolhido
try:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if output_format == 'excel':
        output_path = f"dados_sinteticos_{timestamp}.xlsx"
        df_synth.to_excel(output_path, index=False, sheet_name="Dados Sintéticos")
        logger.log_success(f"Dados exportados para Excel: {output_path}")
        
    elif output_format == 'csv':
        output_path = f"dados_sinteticos_{timestamp}.csv"
        df_synth.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.log_success(f"Dados exportados para CSV: {output_path}")
        
    elif output_format == 'delta':
        spark_df = spark.createDataFrame(df_synth)
        table_name = f"main.vendas_regionais.tb_dados_sinteticos_{timestamp}"
        spark_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.log_success(f"Dados exportados para Delta Table: {table_name}")
    
    else:
        logger.log_warning(f"Formato desconhecido: {output_format}. Usando CSV.")
        output_path = f"dados_sinteticos_{timestamp}.csv"
        df_synth.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.log_success(f"Dados exportados para CSV: {output_path}")
        
except Exception as e:
    logger.log_error("❌ Erro ao exportar dados sintéticos")
    logger.error_handler(e, debug_write_mode=False)
    raise
```

---

## Tratamento de Erros

### Exceções Esperadas

**1. ~~FileNotFoundError~~**: N/A no v2.0 (sem arquivos externos)

**2. ValueError**: Parâmetros inválidos (ex: n_registros <= 0)  
**Ação**: Logar erro com descrição do parâmetro, abortar execução

**3. AssertionError**: Validações de qualidade falharam  
**Ação**: Logar detalhes da validação que falhou, abortar execução

**4. MemoryError**: Volume de dados excede memória disponível  
**Ação**: Logar erro, sugerir reduzir `n_registros`

**Padrão de Tratamento**:
```python
try:
    # Operação crítica
except <ExceptionType> as e:
    logger.log_error("Descrição do erro")
    logger.error_handler(e, debug_write_mode=False)
    raise  # Re-lançar para interromper execução
```

---

## Performance

**Volume Esperado**: 100 a 10.000 registros (uso típico)  
**Volume Máximo Testado**: 100.000 registros  
**Tempo Estimado** (v2.0 - sem overhead de leitura de arquivo): 
- 1.000 registros: < 0.5 segundo
- 10.000 registros: < 3 segundos
- 100.000 registros: < 20 segundos

**Otimizações v2.0**:
- Metadata pré-carregado (zero overhead de IO)
- Uso de NumPy para geração vetorizada
- Evitar loops Python quando possível
- Operações pandas eficientes (map, vectorized operations)

**Gargalos Potenciais**:
- Export para Excel (mais lento que CSV)
- Conversão Pandas → Spark (se usar Delta)

---

## Dependências

### Bibliotecas Python
```python
import pandas as pd        # >= 1.3.0 (manipulação de DataFrames)
import numpy as np         # >= 1.21.0 (geração de números aleatórios)
from datetime import datetime, timedelta  # (manipulação de datas)
import openpyxl            # >= 3.0.0 (export para .xlsx, opcional)
```

### Features SDD
- **error_handler_logging (EHL)**: LogControl centralizado
  - Path: `/data-in-code/error_handler_logging/src/logger_control`

### Dados
- **Nenhuma dependência de dados externos** ✅ (v2.0)
- Metadata pré-configurado no código (IMPL-SDG-C04)

---

## Testes Requeridos

### Testes Unitários
1. **Teste de Volume**: Gerar 100, 1.000, 10.000 registros e validar contagem
2. **Teste de Distribuição**: Validar proporções de Região e Seção (±10%)
3. **Teste de Consistência**: Validar mapeamento código-vendedor e mês-data
4. **Teste de Intervalo**: Validar que vendas estão no range esperado
5. **Teste de Reprodução**: Gerar duas vezes com mesmo seed, validar igualdade
6. **Teste de Aleatoriedade**: Gerar duas vezes sem seed, validar diferença
7. **Teste de Independência (v2.0)**: Executar sem nenhum arquivo externo, validar sucesso

### Testes de Integração
8. **Teste de Export Excel**: Exportar e re-ler arquivo Excel
9. **Teste de Export CSV**: Exportar e re-ler arquivo CSV
10. **Teste de Export Delta**: Exportar para Delta e consultar via SQL
11. **Teste de Logging**: Validar que logs foram persistidos na tabela

### Testes de Performance (v2.0 - sem overhead de IO)
12. **Benchmark 1k**: Executar com 1.000 registros, validar < 1 segundo
13. **Benchmark 10k**: Executar com 10.000 registros, validar < 5 segundos

---

## Parâmetros da Feature

| Parâmetro | Tipo | Obrigatório | Default | Descrição |
|------------|------|-------------|---------|-------------|
| `n_registros` | int | Sim | - | Quantidade de registros a gerar (> 0) |
| `random_seed` | int | Não | None | Seed para reprodução determinística |
| `output_format` | str | Não | "csv" | Formato de saída: "csv", "excel", "delta" |

**Exemplo de Uso**:
```python
# Gerar 5.000 registros com reprodução
df_synth = gerar_dados_sinteticos(
    n_registros=5000,
    random_seed=42,
    output_format="csv"
)
```

---

## Melhorias Futuras (Pós-v2.0)

1. **Múltiplos perfis de metadata**: Permitir escolher diferentes perfis pré-configurados
2. **Distribuições customizáveis**: Permitir usuário especificar distribuições
3. **Adição de ruído controlado**: Adicionar variações controladas nos dados
4. **Geração incremental**: Adicionar dados sintéticos a dataset existente
5. **UI/Dashboard**: Interface gráfica para configuração e visualização

---

## Histórico de Versões

| Versão | Data | Mudanças |
|--------|------|----------|
| 1.0 | 2026-04-04 | Versão inicial com dependência de arquivo Excel (SPEC-SDG-R01: Leitura de Dados Originais) |
| 2.0 | 2026-04-04 | **Metadata pré-configurado** - 100% independente de arquivos externos (SPEC-SDG-R01: Metadata Pré-configurado) |

---

**Status**: ✅ Especificação Completa (v2.0)  
**Próximo Documento**: `tasks.md` (Checklist de Implementação)