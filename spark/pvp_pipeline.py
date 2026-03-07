"""
PvP Pipeline — Spark Structured Streaming
pw_analytics · Data Engineering PoC

Medallion Architecture:
  [Bronze]  Kafka raw messages       — pvp_kills topic (retido 24h)
  [Silver]  Parsed kill events       — Delta Lake /data/pvp_events_delta
                                        MERGE idempotente, sem duplicatas
  [Gold]    Aggregated player stats  — PostgreSQL pvp_stats (serving layer)
                                        incremental UPSERT via ON CONFLICT

Flow:
  Kafka ──► foreachBatch ──► write_silver() ──► Delta Lake (MERGE)
                         └─► write_gold()   ──► PostgreSQL pvp_stats
                                            └─► PostgreSQL pvp_events (guard)
"""

import json
import os
import re
import time as _time
from datetime import datetime, timezone

import psycopg2
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

# ── Configuração ──────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PG_HOST       = os.getenv("PG_HOST",     "postgres")
PG_PORT       = int(os.getenv("PG_PORT", "5432"))
PG_DB         = os.getenv("PG_DB",       "tabacaria_dw")
PG_USER       = os.getenv("PG_USER",     "tabacaria")
PG_PASS       = os.getenv("PG_PASS",     "tabacaria123")
DELTA_PATH    = "/data/pvp_events_delta"
CHECKPOINT    = "/tmp/checkpoints/pvp"

# ── Schemas ───────────────────────────────────────────────────────────────────

MSG_SCHEMA = StructType([
    StructField("id",      StringType()),
    StructField("content", StringType()),
])

# Silver schema: cada linha é um kill event parsed
SILVER_SCHEMA = StructType([
    StructField("msg_id",       LongType()),
    StructField("hora",         StringType()),
    StructField("cla_kill",     StringType()),
    StructField("nick_kill",    StringType()),
    StructField("cla_death",    StringType()),
    StructField("nick_death",   StringType()),
    StructField("processed_at", TimestampType()),
])

# ── Kill event regex ──────────────────────────────────────────────────────────
# Formato da mensagem Discord:
#   :crossed_swords: `18:17` <:emoji:id> `[Clan]`**Nick** matou <:emoji:id> `[Clan]`**Nick**
KILL_PATTERN = re.compile(
    r":crossed_swords:\s+`(\d{2}:\d{2})`"
    r"\s+<[^>]+>\s+`\[(.+?)\]`\*\*(.+?)\*\*"
    r"\s+matou\s+"
    r"<[^>]+>\s+`\[(.+?)\]`\*\*(.+?)\*\*"
)


# ── Logging estruturado ───────────────────────────────────────────────────────

def log(event: str, **kwargs) -> None:
    print(json.dumps(
        {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "service": "pvp_pipeline", "event": event, **kwargs},
        ensure_ascii=False,
    ), flush=True)


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )


def ensure_schema(conn) -> None:
    """Migrações de schema idempotentes — roda no startup do pipeline."""
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE pvp_events
            ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ DEFAULT NOW()
        """)
    conn.commit()
    log("schema_ok")


# ── Silver Layer — Delta Lake ─────────────────────────────────────────────────

def write_silver(events_df: DataFrame, spark: SparkSession) -> int:
    """
    Escreve kill events no Delta Lake usando MERGE (upsert).
    Garante que reprocessamentos (falhas, restart do Spark) não dupliquem
    registros — a chave de deduplicação é (msg_id, nick_kill, nick_death, hora).
    """
    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        delta = DeltaTable.forPath(spark, DELTA_PATH)
        (
            delta.alias("t")
            .merge(
                events_df.alias("s"),
                "t.msg_id    = s.msg_id"
                " AND t.nick_kill  = s.nick_kill"
                " AND t.nick_death = s.nick_death"
                " AND t.hora       = s.hora",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Primeira escrita — cria a tabela Delta
        events_df.write.format("delta").mode("append").save(DELTA_PATH)

    return events_df.count()


# ── Gold Layer — PostgreSQL ───────────────────────────────────────────────────

def write_gold(cur, msg_id: int, hora: str,
               cla_kill: str, nick_kill: str,
               cla_death: str, nick_death: str) -> bool:
    """
    Gold layer: upsert incremental em pvp_stats.
    Guard de idempotência via UNIQUE(msg_id, nick_kill, nick_death, hora)
    em pvp_events — eventos já processados retornam False.

    Kill streak tracking:
      cur_kill_streak  = streak ativa (zera na morte)
      kill_streak      = máximo histórico (nunca decresce)
    """
    # Guard — detecta e rejeita duplicatas
    cur.execute("""
        INSERT INTO pvp_events (msg_id, hora, cla_kill, nick_kill, cla_death, nick_death)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (msg_id, nick_kill, nick_death, hora) DO NOTHING
    """, (msg_id, hora, cla_kill, nick_kill, cla_death, nick_death))

    if cur.rowcount == 0:
        return False  # duplicata — não atualiza pvp_stats

    # Killer: kills++, streak ativa++, atualiza máximo, zera death streak
    cur.execute("""
        INSERT INTO pvp_stats
            (nick, clan, kills, deaths, kd, kill_streak, death_streak,
             cur_kill_streak, cur_death_streak)
        VALUES (%s, %s, 1, 0, 1.0, 1, 0, 1, 0)
        ON CONFLICT (nick) DO UPDATE SET
            clan             = EXCLUDED.clan,
            kills            = pvp_stats.kills + 1,
            cur_kill_streak  = pvp_stats.cur_kill_streak + 1,
            kill_streak      = GREATEST(pvp_stats.kill_streak,
                                        pvp_stats.cur_kill_streak + 1),
            cur_death_streak = 0,
            kd               = ROUND((pvp_stats.kills + 1)::numeric
                                     / GREATEST(pvp_stats.deaths, 1), 2)
    """, (nick_kill, cla_kill))

    # Victim: deaths++, streak ativa++, atualiza máximo, zera kill streak
    cur.execute("""
        INSERT INTO pvp_stats
            (nick, clan, kills, deaths, kd, kill_streak, death_streak,
             cur_kill_streak, cur_death_streak)
        VALUES (%s, %s, 0, 1, 0.0, 0, 1, 0, 1)
        ON CONFLICT (nick) DO UPDATE SET
            clan             = EXCLUDED.clan,
            deaths           = pvp_stats.deaths + 1,
            cur_death_streak = pvp_stats.cur_death_streak + 1,
            death_streak     = GREATEST(pvp_stats.death_streak,
                                        pvp_stats.cur_death_streak + 1),
            cur_kill_streak  = 0,
            kd               = ROUND(pvp_stats.kills::numeric
                                     / GREATEST(pvp_stats.deaths + 1, 1), 2)
    """, (nick_death, cla_death))

    return True


# ── Batch processor ───────────────────────────────────────────────────────────

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    foreachBatch handler — executa por micro-batch do Structured Streaming.
    Ordem: Bronze parse → Silver (Delta MERGE) → Gold (PostgreSQL UPSERT)
    """
    t0 = _time.time()
    rows = batch_df.collect()
    processed_at = datetime.now(timezone.utc)

    # ── Bronze → parse kill events ────────────────────────────────────────────
    events = []
    for row in rows:
        msg_id  = int(row["id"] or 0)
        content = row["content"] or ""
        for line in re.split(r"[\r\n]+", content):
            m = KILL_PATTERN.search(line)
            if m:
                hora, cla_kill, nick_kill, cla_death, nick_death = m.groups()
                events.append((msg_id, hora, cla_kill, nick_kill, cla_death,
                                nick_death, processed_at))

    if not events:
        log("batch_empty", batch_id=batch_id, messages=len(rows))
        return

    spark = batch_df.sparkSession

    # ── Silver: Delta Lake MERGE (idempotente, sem duplicatas) ───────────────
    events_df = spark.createDataFrame(events, SILVER_SCHEMA)
    silver_count = write_silver(events_df, spark)
    delta_ms = round((_time.time() - t0) * 1000)

    # ── Gold: PostgreSQL upsert incremental ───────────────────────────────────
    conn = pg_connect()
    new_events = 0
    try:
        with conn.cursor() as cur:
            for ev in events:
                if write_gold(cur, *ev[:6]):   # processed_at não vai pro gold guard
                    new_events += 1
        conn.commit()
    except Exception as e:
        conn.rollback()
        log("gold_error", error=str(e), batch_id=batch_id)
        raise
    finally:
        conn.close()

    log("batch_processed",
        batch_id=batch_id,
        messages=len(rows),
        kills_parsed=len(events),
        kills_new=new_events,
        silver_records=silver_count,
        delta_ms=delta_ms,
        total_ms=round((_time.time() - t0) * 1000))


# ── Pipeline entrypoint ───────────────────────────────────────────────────────

def run_pipeline(spark: SparkSession) -> None:
    # Garante schema atualizado antes de processar
    conn = pg_connect()
    try:
        ensure_schema(conn)
    finally:
        conn.close()

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "pvp_kills")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), MSG_SCHEMA).alias("d"))
        .select("d.id", "d.content")
    )

    query = (
        stream.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT)
        .start()
    )

    log("started", kafka=KAFKA_SERVERS, delta=DELTA_PATH, trigger="10s")
    query.awaitTermination()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("PvPPipeline")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_pipeline(spark)
