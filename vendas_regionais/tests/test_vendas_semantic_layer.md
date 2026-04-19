# Tests: Vendas Semantic Layer

**Path**: `tests/test_vendas_semantic_layer.md`  
**Feature**: vendas_semantic_layer  
**Documentação**: `sdd/features/vendas_semantic_layer/`  
**Código testado**: `src/nb_vendas_semantic_layer.py`

---

## 🎯 Test Strategy

This test suite validates the semantic layer SQL views against expected business metrics from the Excel "Base Grafico" sheet.

All paths in this document are relative to the project root: `vendas_regionais/`

## 📋 Test Coverage

### View Creation Tests
* All 4 views created successfully
* Views are queryable
* Schemas match specification

### Data Accuracy Tests
* `vw_vendas_por_vendedor`: Totals match Excel (8 vendors)
* `vw_vendas_por_regiao`: Totals match Excel (4 regions)
* `vw_vendas_por_mes`: Totals match Excel (5 months)
* `vw_vendas_por_secao`: Totals match Excel (8 sections)

### Business Logic Tests
* Percentages sum to 100% (for regiao and secao views)
* Transaction counts are consistent across views
* Grand total is identical across all views (R$ 225,926.23)
* No null values in aggregated columns

### Expected Test Results

| View | Expected Count | Total Vendas | Status |
|------|----------------|--------------|--------|
| vw_vendas_por_vendedor | 8 rows | R$ 225,926.23 | 🔄 Pending |
| vw_vendas_por_regiao | 4 rows | R$ 225,926.23 | 🔄 Pending |
| vw_vendas_por_mes | 5 rows | R$ 225,926.23 | 🔄 Pending |
| vw_vendas_por_secao | 8 rows | R$ 225,926.23 | 🔄 Pending |

## 🚀 How to Run Tests

### Option 1: Direct View Validation (SQL)

Run validation queries directly in a SQL notebook:

```sql
-- Check total consistency
SELECT 'Vendedor' AS fonte, SUM(total_vendas) AS total FROM vw_vendas_por_vendedor
UNION ALL
SELECT 'Região' AS fonte, SUM(total_vendas) AS total FROM vw_vendas_por_regiao;

-- Check percentage sum
SELECT SUM(percentual_total) FROM vw_vendas_por_regiao;
-- Expected: 100.00

-- Check row counts
SELECT 'vw_vendas_por_vendedor' AS view_name, COUNT(*) AS row_count 
FROM vw_vendas_por_vendedor;
-- Expected: 8
```

### Option 2: Python-based Testing (Future)

```python
# TODO: Implement pytest-based test suite
# Location: tests/test_semantic_layer.py
# Run with: pytest tests/test_semantic_layer.py -v
```

### Option 3: Execute Notebook Test Cells

If tests are implemented as cells in `src/nb_vendas_semantic_layer.py`:
1. Open notebook: `src/nb_vendas_semantic_layer.py`
2. Navigate to test section
3. Execute test cells

## ✅ Success Criteria

* All 4 views exist and are queryable
* View row counts match specification
* Grand total consistency across all views
* Percentage calculations sum to 100%
* Individual values match Excel within 0.01 tolerance
* Query performance < 2 seconds per view

## 📊 Sample Validation Queries

### Check Totals Consistency
```sql
-- Should all return R$ 225,926.23
SELECT 'Vendedor' AS fonte, SUM(total_vendas) AS total FROM vw_vendas_por_vendedor
UNION ALL
SELECT 'Região' AS fonte, SUM(total_vendas) AS total FROM vw_vendas_por_regiao
UNION ALL
SELECT 'Mês' AS fonte, SUM(total_vendas) AS total FROM vw_vendas_por_mes
UNION ALL
SELECT 'Seção' AS fonte, SUM(total_vendas) AS total FROM vw_vendas_por_secao;
```

### Check Percentage Sum
```sql
-- Should return 100.00 for both
SELECT 'Região' AS view_name, SUM(percentual_total) AS sum_pct 
FROM vw_vendas_por_regiao
UNION ALL
SELECT 'Seção' AS view_name, SUM(percentual_total) AS sum_pct 
FROM vw_vendas_por_secao;
```

### Check Row Counts
```sql
SELECT 'vw_vendas_por_vendedor' AS view_name, COUNT(*) AS row_count 
FROM vw_vendas_por_vendedor
UNION ALL
SELECT 'vw_vendas_por_regiao', COUNT(*) FROM vw_vendas_por_regiao
UNION ALL
SELECT 'vw_vendas_por_mes', COUNT(*) FROM vw_vendas_por_mes
UNION ALL
SELECT 'vw_vendas_por_secao', COUNT(*) FROM vw_vendas_por_secao;
```

## 📝 Test Development Guidelines

1. Compare all aggregated values against Excel source (`arquivos/VendasRegionaisVBA.xlsm`)
2. Test both happy path and edge cases
3. Validate SQL syntax and performance
4. Document any discrepancies with business justification
5. Use decimal precision for monetary comparisons
6. Log all test executions using LogControl (if running from notebooks)

## 📂 Related Files

* **Feature docs**: `sdd/features/vendas_semantic_layer/` (plan, spec, tasks, traceability)
* **Implementation**: `src/nb_vendas_semantic_layer.py`
* **Source data**: `arquivos/VendasRegionaisVBA.xlsm`
* **Base table**: `main.vendas_regionais.tb_vendas_base` (created by `src/nb_vendas_base_ingestion.py`)

---

**Status**: Test suite to be implemented in future iteration  
**Last Updated**: 2026-04-04 (Paths adjusted to relative format)
