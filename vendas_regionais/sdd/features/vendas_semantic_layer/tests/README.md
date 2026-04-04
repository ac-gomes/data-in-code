# Tests: Vendas Semantic Layer

## 🎯 Test Strategy

This directory contains automated tests to validate the semantic layer SQL views against expected business metrics from the Excel "Base Grafico" sheet.

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

### SQL-based Validation

```sql
-- Run all validation queries
%run /path/to/tests/validate_semantic_views.sql
```

### Python-based Testing

```python
# TODO: Implement pytest-based test suite
# Example:
# pytest tests/test_semantic_layer.py -v
```

## ✅ Success Criteria

* All 4 views exist and are queryable
* View row counts match specification
* Grand total consistency across all views
* Percentage calculations sum to 100%
* Individual values match Excel within 0.01 tolerance
* Query performance < 2 seconds per view

## 📊 Sample Validation Queries

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

## 📝 Test Development Guidelines

1. Compare all aggregated values against Excel source
2. Test both happy path and edge cases
3. Validate SQL syntax and performance
4. Document any discrepancies with business justification
5. Use decimal precision for monetary comparisons

---

**Status**: Test suite to be implemented in future iteration
