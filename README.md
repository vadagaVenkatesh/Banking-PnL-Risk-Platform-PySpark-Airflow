# Banking PnL & Risk Platform (PySpark + Airflow)

Executive Summary
- Goal: Enterprise-grade platform to compute daily PnL, risk, and regulatory metrics at scale across trading and banking books, sourced from the bank’s golden capital data stores (trade, market, reference, liquidity, and capital schedules).
- Scale: Tens of millions of trades, billions of rows of time-series market data; minute-level intraday refresh for critical metrics, daily batch for regulatory aggregates.
- Regulatory scope: Basel III/IV (FRTB, SACCR, RWA), CCAR/DFAST, MiFID II, EMIR, Dodd-Frank. Outputs feed capital planning, IHC/reg entity reporting, and management MI.
- Technology: PySpark for distributed ETL/analytics; Airflow for deterministic orchestration; Delta/Lakehouse for ACID data lake; optional Kafka for real-time streams.

Table of Contents
1. Architecture Overview
2. Modules & Design
   - Trade Capture & Normalization
   - PnL (Daily/Intraday)
   - Stress & Scenario (CCAR/DFAST/ICAAP)
   - Compliance & Controls (MiFID II, EMIR, Dodd-Frank)
   - Reporting & Data Products (MI, Reg, Ad-hoc)
   - IHC/Legal-Entity Views
   - Counterparty Risk: PFE & SACCR
   - Credit/Capital: RWA
   - Orchestration: Airflow Automation
3. Text Architecture Diagram
4. Parallelism & Performance (PySpark, Partitioning, Airflow)
5. Sample Code: PySpark ETL (partitioned)
6. Sample Code: PySpark Risk (PnL, PFE, SACCR)
7. Sample Code: Airflow DAG (daily schedule)
8. Operations & Deployment
9. Basel/Regulatory Mapping
10. Contribution Guide & Next Steps

1) Architecture Overview
- Ingestion: Trade feeds (FO/MO/BO), market data (prices, vol surfaces, curves), static (counterparty, CSA, limits), reference (products, calendars), corporate actions.
- Storage: Lakehouse (Delta/Parquet) on object storage with bronze/silver/gold layers; schema registry for contracts; robust data quality.
- Compute: PySpark on cluster (YARN/K8s/EMR/Dataproc) with autoscaling; broadcast/partition strategies; vectorized UDFs only when necessary.
- Orchestration: Apache Airflow with SLAs, retries, backfills, dataset-triggered runs; lineage and observability via OpenLineage.
- Output: Data products for MI dashboards, regulatory extracts (CSV/XML/XBRL), and downstream risk engines.
- Security: Row/column-level security, secrets via Vault, audit trails, immutable logs.

2) Modules & Design
A. Trade Capture & Normalization
- Inputs: FO trade capture (e.g., Murex/Calypso/Summit/Custom), positions, lifecycle events.
- Normalize to canonical schema: trade_header, trade_leg, cashflows, attributes, book_desk, counterparty.
- Controls: schema validation, completeness (record counts), referential checks (book, product, cpty), duplicate prevention (trade_id+version), late-arrival logic.
- Outputs: Silver layer normalized tables partitioned by as_of_date, legal_entity, book.

B. PnL (Daily/Intraday)
- Daily PnL decomposition: Open/Close, Price PnL, Carry/Theta, FX, New/Cancel/Modify effects.
- Intraday refresh for sensitive books; final EOD close with locks and sign-off flags.
- Inputs: previous EOD positions, today positions, bump-sensitive market data, FX.
- Outputs: pnl_daily, pnl_explained, pnl_attribution_by_desk, with controls vs desk sign-off.

C. Stress & Scenario (CCAR/DFAST/ICAAP)
- Deterministic scenarios (Fed severely adverse, firm-wide idiosyncratic) and historical replay.
- Shock engines for rates, credit spreads, equities, FX, commodities; correlation and liquidity horizons.
- Write-down paths, PPNR/OCI hooks, horizon aggregation (9Q/13Q) for CCAR.

D. Compliance & Controls (MiFID II, EMIR, Dodd-Frank)
- Transaction reporting views, best-execution metrics, EMIR trade-state transitions, UTI/LEI enrichment.
- Exception management queues, re-reporting workflow, and immutable audit with actor/timestamp.

E. Reporting & Data Products
- Management MI: desk PnL ladders, VaR/PFE bands, limit utilization.
- Reg extracts: CSV/XML conforming to reporting templates; per-entity delivery with checksum and versioning.
- Ad-hoc sandbox with governed self-serve tables and masking.

F. IHC/Legal-Entity Views
- Entity mapping tree (consolidated -> IHC -> legal entities -> branches).
- Filters and eliminations to produce stand-alone IHC books; currency translation policies.

G. Counterparty Risk: PFE & SACCR
- PFE via Monte Carlo exposure profiles with collateral and netting; percentile ladders.
- SACCR per BCBS d424: replacement cost (RC), potential future exposure (PFE add-on), multiplier, netting set aggregation.
- Output: exposure_by_netting_set, saccr_components, ead_saccr, ead_pit.

H. Credit/Capital: RWA
- Compute RWA for credit risk (STD/IRB), market risk (FRTB-SA, optional IMA hooks), and CVA.
- Produce capital ratios and buffers, link to management overlays.

I. Orchestration: Airflow Automation
- DAGs per domain with datasets and cross-DAG dependencies; SLAs, retries with exponential backoff; on-failure callbacks to pager.

3) Text Architecture Diagram
[Producers]
  - Trade FO/MO/BO systems
  - Market data (prices, curves, vols)
  - Reference/static (cpty, CSA, products)
       |
       v
[Airflow Ingestion DAGs] -> [Bronze Delta Tables]
       |
       v
[Validation & Normalization Jobs (PySpark)] -> [Silver Delta Tables]
       |
       v
[Risk Engines (PnL, PFE, SACCR, RWA) - PySpark] -> [Gold Views/Extracts]
       |
       +--> [MI Dashboards]
       +--> [Regulatory Extracts (IHC/CCAR/EMIR/MiFID)]
       +--> [Downstream Systems]

4) Parallelism & Performance
- Partitioning: by as_of_date (daily), legal_entity, desk/book; for market data, by as_of_date and symbol/bucket.
- File sizing: 128–512 MB Parquet target; optimize + Z-order on frequently filtered columns.
- Joins: broadcast small dims; use join hints judiciously; avoid skew via salting on heavy keys (cpty_id, netting_set_id).
- Caching: persist intermediate narrow stages; unpersist aggressively.
- Airflow: parallel task groups per entity/desk; pools to cap external system pressure; datasets to trigger incremental runs.
- Monitoring: Spark listener metrics, Airflow task SLAs, data quality SLAs with circuit breakers.

5) Sample Code: PySpark ETL (Partitioned)
```python
from pyspark.sql import SparkSession, functions as F, types as T
from delta.tables import DeltaTable

spark = (SparkSession.builder.appName("banking-pnl-risk-etl")
         .config("spark.sql.shuffle.partitions", "600")
         .config("spark.databricks.delta.optimizeWrite.enabled", "true")
         .getOrCreate())

as_of = spark.conf.get("etl.as_of_date")  # e.g. 2025-09-30

raw = (spark.read.option("header", True)
       .schema("trade_id string, version int, book string, product string, cpty_id string, notional double, ccypair string, trade_dt date, as_of_date date")
       .csv(f"s3://bank-bronze/trades/as_of_date={as_of}/*"))

# Data quality
dq = (raw.withColumn("dq_missing_keys", F.when(F.col("trade_id").isNull() | F.col("book").isNull(), F.lit(1)).otherwise(0))
          .withColumn("dq_valid_version", F.when(F.col("version") >= 0, 1).otherwise(0))
          .filter(F.col("dq_missing_keys") == 0))

# Deduplicate on (trade_id, as_of_date) by max version
w = (Window.partitionBy("trade_id", "as_of_date").orderBy(F.col("version").desc()))
latest = (dq.withColumn("rn", F.row_number().over(w))
             .filter(F.col("rn") == 1)
             .drop("rn"))

# Write to Delta partitioned by as_of_date and book
(target := f"s3://bank-silver/trades").__class__  # py3.8-friendly no-op to show target
(latest.write.format("delta")
       .mode("overwrite")
       .partitionBy("as_of_date", "book")
       .save("s3://bank-silver/trades"))

# Optimize & vacuum handled by separate maintenance job
```

6) Sample Code: PySpark Risk (PnL, PFE, SACCR)
```python
from pyspark.sql import functions as F, Window

as_of = spark.conf.get("risk.as_of_date")
positions = spark.read.format("delta").load("s3://bank-silver/positions").filter(F.col("as_of_date") == as_of)
md = spark.read.format("delta").load("s3://bank-silver/market").filter(F.col("as_of_date") == as_of)
fx = spark.read.format("delta").load("s3://bank-silver/fx").filter(F.col("as_of_date") == as_of)

# Price PnL (simplified): dV ≈ Greeks * dFactors + residual
joined = (positions.join(md, ["symbol"], "left")
                    .join(fx.select("ccy", F.col("rate").alias("fx_rate")), positions.ccy == F.col("ccy"), "left"))

pnl = (joined.withColumn("price_pnl", F.col("delta") * F.col("d_price") + 0.5 * F.col("gamma") * F.col("d_price")**2)
              .withColumn("fx_pnl", (F.col("value_local") / F.col("fx_rate")) - F.col("value_reporting"))
              .groupBy("as_of_date", "legal_entity", "book")
              .agg(F.sum("price_pnl").alias("price_pnl"), F.sum("fx_pnl").alias("fx_pnl")))

# SACCR components (very simplified mapping)
netting = spark.read.format("delta").load("s3://bank-silver/netting_sets")
rc = positions.groupBy("netting_set_id").agg(F.sum("mtm").alias("mtm_net"), F.sum("collateral").alias("coll_net"))
rc = rc.withColumn("RC", F.greatest(F.col("mtm_net") - F.col("coll_net"), F.lit(0.0)))

# Add-on proxy by asset class weights (placeholder values should be replaced per BCBS tables)
add_on = (positions.groupBy("netting_set_id", "asset_class")
                  .agg(F.sum(F.abs(F.col("notional"))).alias("gross_notional"))
                  .withColumn("addon_weight", F.when(F.col("asset_class") == "IR", 0.005)
                                              .when(F.col("asset_class") == "FX", 0.04)
                                              .when(F.col("asset_class") == "EQ", 0.08)
                                              .otherwise(0.1))
                  .withColumn("AddOn_component", F.col("gross_notional") * F.col("addon_weight"))
                  .groupBy("netting_set_id").agg(F.sum("AddOn_component").alias("AddOn")))

saccr = (rc.join(add_on, "netting_set_id")
           .withColumn("multiplier", F.lit(1.5))  # simplified; replace with supervisory formula
           .withColumn("PFE", F.col("multiplier") * F.col("AddOn"))
           .withColumn("EAD", F.col("RC") + F.col("PFE")))

pnl.write.format("delta").mode("overwrite").save("s3://bank-gold/pnl_daily/as_of_date=" + as_of)
saccr.write.format("delta").mode("overwrite").save("s3://bank-gold/saccr/as_of_date=" + as_of)
```

7) Sample Code: Airflow DAG (Daily)
```python
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "risk-tech",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
    "email": ["risk-oncall@bank.com"],
}

with DAG(
    dag_id="daily_pnl_risk",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 2 * * *",  # 02:00 UTC after market close
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=2,
    tags=["risk", "pnl", "basel"],
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_trades = SparkSubmitOperator(
        task_id="ingest_trades",
        application="s3://jobs/ingest_trades.py",
        conf={"spark.yarn.maxAppAttempts": "1"},
        application_args=["--as_of_date", "{{ ds }}"],
        executor_cores=4, executor_memory="8g", num_executors=50
    )

    normalize_trades = SparkSubmitOperator(
        task_id="normalize_trades",
        application="s3://jobs/normalize_trades.py",
        application_args=["--as_of_date", "{{ ds }}"],
        num_executors=80
    )

    market_etl = SparkSubmitOperator(
        task_id="market_etl",
        application="s3://jobs/market_etl.py",
        application_args=["--as_of_date", "{{ ds }}"],
        num_executors=60
    )

    pnl_compute = SparkSubmitOperator(
        task_id="pnl_compute",
        application="s3://jobs/pnl_compute.py",
        application_args=["--as_of_date", "{{ ds }}"],
        num_executors=120
    )

    saccr_compute = SparkSubmitOperator(
        task_id="saccr_compute",
        application="s3://jobs/saccr_compute.py",
        application_args=["--as_of_date", "{{ ds }}"],
        num_executors=120
    )

    end = EmptyOperator(task_id="end")

    start >> [ingest_trades, market_etl] >> normalize_trades
    [normalize_trades, market_etl] >> pnl_compute >> saccr_compute >> end
```

8) Operations & Deployment
- Environments: dev -> uat -> prod with data contracts and promotion gates; feature flags for risk engines.
- CI/CD: unit + schema tests (Great Expectations), Spark job tests (pytest + local[2]), DAG validation; image build with pinned Spark/Delta versions.
- Secrets: Airflow connections from Vault; S3/ADLS credentials via instance profiles/managed identities.
- Observability: Airflow SLAs, task instance logs to S3, Spark history server; data quality KPIs published to Grafana.
- Backfills: parameterized as_of_date; use Airflow backfill with limited concurrency and guardrails.
- Cost & Scale: autoscaling clusters; spot/preemptible where safe; optimize file sizes; prune small files; broadcast joins; Z-order.

9) Basel/Regulatory Mapping (selected)
- FRTB (BCBS 457/352): trading book boundary, risk-theoretical PnL attribution hooks, SA risk charges as default.
