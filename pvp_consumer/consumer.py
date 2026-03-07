"""
PvP Consumer — Python Kafka consumer (substitui pvp-spark)
Lê do tópico pvp_kills, parseia kill events e escreve no PostgreSQL.

Flow:
  Kafka pvp_kills ──► parse kill events ──► pvp_events (guard idempotência)
                                        └──► pvp_stats  (upsert incremental)
"""

import json
import os
import re
import time
from datetime import datetime, timezone

DISCORD_EPOCH = 1420070400000

def snowflake_to_hora(snowflake_id: int) -> str:
    ms = (snowflake_id >> 22) + DISCORD_EPOCH
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.strftime("%H:%M")

import psycopg2
from kafka import KafkaConsumer

# ── Configuração ──────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "pvp_kills")
PG_HOST       = os.getenv("PG_HOST",     "postgres")
PG_PORT       = int(os.getenv("PG_PORT", "5432"))
PG_DB         = os.getenv("PG_DB",       "tabacaria_dw")
PG_USER       = os.getenv("PG_USER",     "tabacaria")
PG_PASS       = os.getenv("PG_PASS",     "tabacaria123")

# ── Kill event regex ──────────────────────────────────────────────────────────
# Formato text:  :crossed_swords: `18:17` <emoji> `[Clan]`**Nick** matou <emoji> `[Clan]`**Nick**
# Formato embed: :crossed_swords: <emoji> <emoji> `[Clan]`**Nick** matou <emoji> <emoji> `[Clan]`**Nick**
KILL_PATTERN = re.compile(
    r":crossed_swords:\s+"
    r"(?:`(\d{2}:\d{2})`\s+)?"   # hora opcional (group 1)
    r"(?:<[^>]+>\s+)*"            # 0+ emojis antes do killer
    r"`\[(.+?)\]`\*\*(.+?)\*\*"  # [Clan]**Nick** killer (groups 2, 3)
    r"\s+matou\s+"
    r"(?:<[^>]+>\s+)*"            # 0+ emojis antes da vítima
    r"`\[(.+?)\]`\*\*(.+?)\*\*"  # [Clan]**Nick** vítima (groups 4, 5)
)


# ── Logging estruturado ───────────────────────────────────────────────────────

def log(event: str, **kwargs) -> None:
    print(json.dumps(
        {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "service": "pvp_consumer", "event": event, **kwargs},
        ensure_ascii=False,
    ), flush=True)


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )


def write_event(cur, msg_id: int, hora: str,
                cla_kill: str, nick_kill: str,
                cla_death: str, nick_death: str) -> bool:
    """
    Guard de idempotência via UNIQUE(msg_id, nick_kill, nick_death, hora).
    Retorna True se o evento é novo (foi inserido), False se duplicata.
    """
    cur.execute("""
        INSERT INTO pvp_events (msg_id, hora, cla_kill, nick_kill, cla_death, nick_death)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (msg_id, nick_kill, nick_death, hora) DO NOTHING
    """, (msg_id, hora, cla_kill, nick_kill, cla_death, nick_death))

    if cur.rowcount == 0:
        return False  # duplicata

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


# ── Consumer loop ─────────────────────────────────────────────────────────────

def run():
    log("starting", kafka=KAFKA_SERVERS, topic=KAFKA_TOPIC)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    log("connected")

    conn = pg_connect()
    total_new = 0

    for message in consumer:
        data = message.value
        msg_id  = int(data.get("id") or 0)
        content = data.get("content") or ""

        events_found = 0
        events_new   = 0

        try:
            with conn.cursor() as cur:
                for line in re.split(r"[\r\n]+", content):
                    m = KILL_PATTERN.search(line)
                    if not m:
                        continue
                    hora, cla_kill, nick_kill, cla_death, nick_death = m.groups()
                    if not hora:
                        hora = snowflake_to_hora(msg_id)
                    events_found += 1
                    if write_event(cur, msg_id, hora, cla_kill, nick_kill, cla_death, nick_death):
                        events_new += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            log("db_error", error=str(e), msg_id=msg_id)
            # Reconecta se a conexão foi perdida
            try:
                conn.close()
            except Exception:
                pass
            conn = pg_connect()
            continue

        if events_found:
            total_new += events_new
            log("kills_processed",
                msg_id=msg_id,
                kills_found=events_found,
                kills_new=events_new,
                total_new=total_new)
        elif total_new == 0 and message.offset < 5:
            # Debug: mostra sample das primeiras mensagens para diagnóstico
            log("msg_sample", offset=message.offset, msg_id=msg_id,
                content_head=(content[:120] if content else "(vazio)"))


if __name__ == "__main__":
    # Aguarda Kafka ficar disponível
    for attempt in range(30):
        try:
            run()
            break
        except Exception as e:
            log("startup_error", attempt=attempt + 1, error=str(e))
            time.sleep(5)
