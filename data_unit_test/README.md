[pt-br]
# Test unitário para notebooks Databricks, Apache Spark e Microsoft Fabric
 - Testes inspirados no DBT & Great Expectations

## Pytest Get Started
https://docs.pytest.org/en/stable/getting-started.html

## unittest — Unit testing framework
https://docs.python.org/3/library/unittest.html

## DBT:
- Framework SQL-first para testes de transformação de dados como parte do pipeline

###  Quando executar:
- Roda como parte do dbt ```dbt run/test```

### Uso principal:
- Garantir consistência dos modelos transformados

### Doc
https://docs.getdbt.com/docs/build/data-tests

## Great Expectations:
- Biblioteca Python independente para testes de qualidade de dados (validações)

### Quando executar:
- Pode rodar a qualquer momento (manual/automatizado)

### Uso principal:
- Monitoramento de qualidade de dados (validations)

[en]
# Unit testing for Databricks notebooks, Apache Spark, and Microsoft Fabric
- Tests inspired by DBT & Great Expectations

### DBT:
SQL-first framework for testing data transformations as part of the pipeline

### When to run:
Executed as part of the DBT workflow using dbt run/test

### Main use case:
Ensure consistency of transformed models

### Doc
https://docs.getdbt.com/docs/build/data-tests

## Great Expectations:
- Independent Python library for data quality testing (validations)
### When to run:
- Can be run at any time (manual or automated)
### Main use case:
- Data quality monitoring (validations)

### Doc
https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/uniqueness
