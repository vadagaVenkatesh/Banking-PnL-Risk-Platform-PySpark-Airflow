# Airflow DAGs - Banking PnL & Risk Platform

This directory contains Apache Airflow DAG definitions for orchestrating the Banking PnL & Risk Platform workflows.

## Overview

The DAGs in this directory coordinate PySpark jobs that process banking data for profit/loss calculations, risk analytics, regulatory compliance, and reporting.

## Current DAGs

### 1. `banking_pnl_risk_pipeline_dag.py`
**Purpose**: Main orchestration DAG for the end-to-end banking PnL and risk pipeline

**Key Components**:
- Trade data ingestion and validation
- PnL computation across portfolios
- Risk metrics calculation (VaR, Expected Shortfall)
- Stress testing scenarios
- RWA (Risk-Weighted Assets) computation
- Counterparty risk analysis
- Regulatory compliance checks
- IHC (Intermediate Holding Company) view generation
- Report generation and distribution

**Schedule**: Daily (configurable)

**Dependencies**: All PySpark modules in `pyspark_modules/`

## Planned DAGs

### 2. `intraday_pnl_monitoring_dag.py` (Planned)
- Real-time/near-real-time PnL monitoring
- Intraday position tracking
- Alert generation for significant P&L movements

### 3. `regulatory_reporting_dag.py` (Planned)
- CCAR/DFAST reporting workflows
- Basel III compliance reporting
- FR Y-14 submissions
- Scheduled regulatory report generation

### 4. `stress_testing_suite_dag.py` (Planned)
- Comprehensive stress testing scenarios
- Historical scenario replay
- Custom shock scenarios
- Reverse stress testing

### 5. `data_quality_validation_dag.py` (Planned)
- Pre-processing data quality checks
- Post-processing validation
- Data reconciliation across sources
- Anomaly detection

### 6. `counterparty_exposure_dag.py` (Planned)
- Daily counterparty exposure calculations
- Credit limit monitoring
- CVA (Credit Valuation Adjustment) computation
- Wrong-way risk identification

### 7. `model_backtesting_dag.py` (Planned)
- VaR model backtesting
- PnL attribution analysis
- Model performance metrics
- Hypothesis testing for model validation

### 8. `ihc_quarterly_reporting_dag.py` (Planned)
- Quarterly IHC reporting workflows
- Aggregated risk views
- Capital adequacy reporting
- Liquidity coverage ratio (LCR) calculations

## Development Guidelines

### Adding a New DAG

1. **File Naming**: Use descriptive names with `_dag.py` suffix
2. **DAG ID**: Use clear, hierarchical naming (e.g., `banking_pnl_risk.pipeline.daily`)
3. **Documentation**: Include comprehensive docstrings explaining:
   - Purpose and business context
   - Dependencies and prerequisites
   - Expected inputs and outputs
   - Error handling strategy
4. **Configuration**: Use Airflow Variables or Connections for environment-specific configs
5. **Testing**: Include unit tests for custom operators/sensors

### DAG Structure Best Practices

```python
"""
DAG Template Example
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments for all tasks
default_args = {
    'owner': 'risk-platform-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='example_dag',
    default_args=default_args,
    description='Brief description',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['banking', 'risk', 'pnl'],
) as dag:
    # Task definitions
    pass
```

## PySpark Module Integration

Each DAG should reference the appropriate PySpark modules from `../pyspark_modules/`:

- `pnl_computation.py` - P&L calculations
- `stress_testing.py` - Stress test scenarios
- `compliance_framework.py` - Regulatory compliance checks
- `reporting.py` - Report generation
- `ihc_views.py` - IHC view aggregation
- `counterparty_risk.py` - Counterparty exposure analysis
- `rwa_computation.py` - Risk-weighted asset calculations

## Parallel Development

This repository is structured to support parallel contribution across different business functions:

1. **Team Assignment**: Each DAG can be owned by a specific team/developer
2. **Independent Development**: DAGs can be developed and tested independently
3. **Code Review**: Each DAG should undergo peer review before merging
4. **Branch Strategy**: Use feature branches (`feature/dag-name`) for new DAGs

## Testing

### Local Testing
```bash
# Test DAG syntax
python airflow_dags/your_dag.py

# List tasks
airflow tasks list <dag_id>

# Test individual task
airflow tasks test <dag_id> <task_id> <execution_date>
```

### Integration Testing
- Use Airflow's test mode for end-to-end validation
- Verify task dependencies and execution order
- Confirm data quality at each stage

## Monitoring and Alerting

- All DAGs should implement appropriate SLAs
- Configure email alerts for failures
- Use Airflow UI for monitoring execution status
- Integrate with external monitoring tools (Prometheus, Grafana, etc.)

## Contact

For questions or contributions, please contact the Risk Platform Team.

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- Internal banking risk framework documentation
