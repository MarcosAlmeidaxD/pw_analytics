"""
Tabacaria E-commerce — Declarative Pipeline (SDP-style)

Estrutura inspirada no Apache Spark Declarative Pipelines (Spark 4.1+).
Cada camada é declarada como uma função pura:
  bronze_transactions()  →  lê stream bruto do Kafka
  silver_transactions()  →  limpa, tipifica e enriquece
  sink()                 →  grava PostgreSQL live_feed (Grafana)

Pipeline:
  Kafka ──► [bronze] ──► [silver] ──► PostgreSQL live_feed  (Grafana)
"""

import json
import os
import time as _time
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, from_json, to_timestamp,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)


def log(event: str, **kwargs) -> None:
    print(json.dumps(
        {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "service": "pipeline", "event": event, **kwargs}
    ), flush=True)

# ── Configuração ──────────────────────────────────────────────────────────────
KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_URL   = os.getenv("POSTGRES_URL",      "jdbc:postgresql://postgres:5432/tabacaria_dw")
POSTGRES_USER  = os.getenv("POSTGRES_USER",     "tabacaria")
POSTGRES_PASS  = os.getenv("POSTGRES_PASSWORD", "tabacaria123")
JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver":   "org.postgresql.Driver",
    "batchsize": "500",
}

# ── Schema do JSON (Kafka value) ───────────────────────────────────────────────
TX_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("timestamp",      StringType()),
    StructField("customer_id",    StringType()),
    StructField("product_name",   StringType()),
    StructField("category",       StringType()),
    StructField("brand",          StringType()),
    StructField("quantity",       IntegerType()),
    StructField("unit_price",     DoubleType()),
    StructField("total_amount",   DoubleType()),
    StructField("payment_method", StringType()),
    StructField("payment_status", StringType()),
    StructField("state",          StringType()),
    StructField("device",         StringType()),
])


# ═══════════════════════════════════════════════════════════════════════════════
#  CAMADA BRONZE — dados brutos do Kafka, sem transformações
# ═══════════════════════════════════════════════════════════════════════════════
def bronze_transactions(spark: SparkSession) -> DataFrame:
    """
    Lê o stream do tópico Kafka `transactions`.
    Faz apenas o parse do JSON para colunas — zero regras de negócio aqui.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), TX_SCHEMA).alias("d"))
        .select("d.*")
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  CAMADA SILVER — dados limpos, tipados e prontos para consumo
# ═══════════════════════════════════════════════════════════════════════════════
def silver_transactions(bronze: DataFrame) -> DataFrame:
    """
    Transforma o bronze em silver:
      - converte `timestamp` (string) → `event_time` (TIMESTAMP)
      - adiciona `date` (DATE) para particionamento Parquet por dia
      - adiciona `processed_at` para rastreabilidade
    """
    return (
        bronze
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withColumn("date", col("event_time").cast("date"))
        .drop("timestamp")
        .select(
            col("transaction_id"),
            col("event_time"),
            col("date"),
            col("customer_id"),
            col("product_name"),
            col("category"),
            col("brand"),
            col("quantity"),
            col("unit_price"),
            col("total_amount"),
            col("payment_method"),
            col("payment_status"),
            col("state"),
            col("device"),
            current_timestamp().alias("processed_at"),
        )
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  SINK — grava em dois destinos por micro-batch
# ═══════════════════════════════════════════════════════════════════════════════
def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    t0 = _time.time()
    batch_df.cache()
    count = batch_df.count()

    # ── PostgreSQL → tabela live_feed (Grafana) ───────────────────────────────
    t1 = _time.time()
    batch_df.drop("date").write.mode("append").jdbc(POSTGRES_URL, "live_feed", properties=JDBC_PROPS)
    postgres_ms = round((_time.time() - t1) * 1000)

    log("batch_processed",
        batch_id=batch_id,
        records=count,
        postgres_ms=postgres_ms,
        total_ms=round((_time.time() - t0) * 1000))

    batch_df.unpersist()


# ═══════════════════════════════════════════════════════════════════════════════
#  PIPELINE — liga as camadas e executa o stream
# ═══════════════════════════════════════════════════════════════════════════════
def run_pipeline(spark: SparkSession) -> None:
    bronze = bronze_transactions(spark)
    silver = silver_transactions(bronze)

    query = (
        silver.writeStream
        .foreachBatch(write_batch)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/silver")
        .start()
    )

    log("started", kafka=KAFKA_SERVERS, postgres=POSTGRES_URL, trigger_interval="5s")
    query.awaitTermination()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("TabacariaDeclarativePipeline")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_pipeline(spark)
