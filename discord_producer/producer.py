"""
Discord PvP Producer
Polls Discord channel → Kafka topic 'pvp_kills'.

Loop contínuo:
  1. Lê cursor e config do banco a cada ciclo (pvp-control pode alterar em runtime)
  2. GET /messages?after=<last_id> → envia para Kafka → atualiza cursor
  3. Se batch > 0  → poll imediato (sem espera)
  4. Se batch = 0  → idle: dorme POLL_INTERVAL em chunks de 5s
                      (reage a mudanças de config em até 5s)
  5. Se stop_id atingido → entra em idle aguardando nova config
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
_logger = logging.getLogger(__name__)

_MAX_BIGINT = 9223372036854775807  # limite BIGINT do PostgreSQL

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
CHANNEL_ID    = os.getenv("CHANNEL_ID", "1035423433040875590")
START_ID      = min(int(os.getenv("START_ID", "1479225035926540351")), _MAX_BIGINT)
_raw_stop     = int(os.getenv("STOP_ID", "0"))
STOP_ID_ENV   = _raw_stop if _raw_stop <= _MAX_BIGINT else 0
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "pvp_kills")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB",   "tabacaria_dw")
PG_USER = os.getenv("PG_USER", "tabacaria")
PG_PASS = os.getenv("PG_PASS", "tabacaria123")


def log(event: str, **kwargs) -> None:
    _logger.info(json.dumps(
        {"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
         "service": "discord_producer", "event": event, **kwargs},
        ensure_ascii=False,
    ))


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def pg_connect():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASS,
            )
            conn.autocommit = True
            log("postgres_connected")
            return conn
        except psycopg2.OperationalError:
            log("postgres_unavailable", retry_in=3)
            time.sleep(3)


def init_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pvp_cursor (
                id          INTEGER PRIMARY KEY DEFAULT 1,
                last_msg_id BIGINT  DEFAULT 0
            )
        """)
        cur.execute(
            "INSERT INTO pvp_cursor (id, last_msg_id) VALUES (1, %s) ON CONFLICT DO NOTHING",
            (START_ID,),
        )
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pvp_config (
                id            INTEGER PRIMARY KEY DEFAULT 1,
                start_id      BIGINT  DEFAULT 0,
                stop_id       BIGINT  DEFAULT 0,
                poll_interval INTEGER DEFAULT 30
            )
        """)
        cur.execute(
            """INSERT INTO pvp_config (id, start_id, stop_id, poll_interval)
               VALUES (1, %s, %s, %s) ON CONFLICT DO NOTHING""",
            (START_ID, STOP_ID_ENV, POLL_INTERVAL),
        )


def read_cursor(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT last_msg_id FROM pvp_cursor WHERE id = 1")
        row = cur.fetchone()
        return int(row[0]) if row else START_ID


def save_cursor(conn, msg_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE pvp_cursor SET last_msg_id = %s WHERE id = 1", (msg_id,))


def read_config(conn) -> tuple[int, int]:
    """Retorna (stop_id, poll_interval) do banco."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT stop_id, poll_interval FROM pvp_config WHERE id = 1")
            row = cur.fetchone()
            if row:
                return int(row[0]), int(row[1])
    except Exception:
        pass
    return STOP_ID_ENV, POLL_INTERVAL


# ── Kafka ─────────────────────────────────────────────────────────────────────

def connect_kafka() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks="all",
                retries=5,
            )
            log("kafka_connected", broker=KAFKA_SERVERS, topic=KAFKA_TOPIC)
            return producer
        except NoBrokersAvailable:
            log("kafka_unavailable", retry_in=3)
            time.sleep(3)


# ── Discord polling ───────────────────────────────────────────────────────────

def fetch_messages(after_id: int, stop_id: int) -> tuple[list, bool]:
    """
    Busca até 100 mensagens após after_id (ordem cronológica).
    Se stop_id > 0 e uma mensagem atingir o limite, trunca e retorna reached_stop=True.
    """
    resp = requests.get(
        f"https://discord.com/api/v9/channels/{CHANNEL_ID}/messages",
        headers={"Authorization": DISCORD_TOKEN},
        params={"limit": "100", "after": str(after_id)},
        timeout=10,
    )
    if resp.status_code == 429:
        retry_after = resp.json().get("retry_after", 5)
        log("rate_limited", retry_after=retry_after)
        time.sleep(float(retry_after))
        return [], False
    if resp.status_code != 200:
        log("http_error", status=resp.status_code, body=resp.text[:200])
        return [], False

    batch = list(reversed(resp.json()))  # Discord retorna mais-recente primeiro

    if not stop_id:
        return batch, False

    filtered = []
    for msg in batch:
        if int(msg["id"]) >= stop_id:
            log("stop_id_reached", stop_id=stop_id, msg_id=msg["id"])
            return filtered, True
        filtered.append(msg)
    return filtered, False


# ── Idle sleep interruptível ──────────────────────────────────────────────────

def idle_sleep(conn, poll_interval: int, current_stop: int, current_cursor: int) -> bool:
    """
    Dorme em chunks de 5s verificando se config ou cursor mudou.
    Retorna True se algo mudou (sai do idle imediatamente).
    """
    elapsed = 0
    while elapsed < poll_interval:
        time.sleep(min(5, poll_interval - elapsed))
        elapsed += 5
        try:
            new_stop, _ = read_config(conn)
            new_cursor  = read_cursor(conn)
            if new_stop != current_stop or new_cursor != current_cursor:
                return True  # config/cursor mudou — sai do idle
        except Exception:
            pass
    return False


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    conn = pg_connect()
    init_tables(conn)
    producer = connect_kafka()

    stop_id, poll_interval = read_config(conn)
    last_id = read_cursor(conn)

    log("started",
        channel_id=CHANNEL_ID,
        cursor=last_id,
        stop_id=stop_id or "live",
        poll_interval_s=poll_interval)

    while True:
        # ── Re-lê config e cursor do banco a cada ciclo ───────────────────────
        # Isso permite que pvp-control altere START_ID/STOP_ID/cursor em runtime
        new_stop, new_interval = read_config(conn)
        new_cursor = read_cursor(conn)

        if new_stop != stop_id or new_interval != poll_interval:
            stop_id, poll_interval = new_stop, new_interval
            log("config_updated", stop_id=stop_id or "live", poll_interval_s=poll_interval)

        if new_cursor != last_id:
            last_id = new_cursor
            log("cursor_reset", last_id=last_id)

        # ── Fetch ─────────────────────────────────────────────────────────────
        try:
            messages, reached_stop = fetch_messages(last_id, stop_id)
        except Exception as e:
            log("fetch_error", error=str(e))
            time.sleep(5)
            continue

        # ── Publish ───────────────────────────────────────────────────────────
        if messages:
            for msg in messages:
                # Combina content + descrições de embeds em um único texto
                parts = [msg.get("content", "")]
                for embed in msg.get("embeds", []):
                    if embed.get("title"):
                        parts.append(embed["title"])
                    if embed.get("description"):
                        parts.append(embed["description"])
                    for field in embed.get("fields", []):
                        parts.append(f"{field.get('name','')} {field.get('value','')}")
                full_text = "\n".join(p for p in parts if p)
                producer.send(KAFKA_TOPIC, value={
                    "id":      msg["id"],
                    "content": full_text,
                })
                last_id = int(msg["id"])

            producer.flush()
            save_cursor(conn, last_id)
            log("batch_sent",
                count=len(messages),
                last_id=last_id,
                stop_id=stop_id or "live")

        # ── Controle de fluxo ─────────────────────────────────────────────────
        if reached_stop:
            log("waiting_new_config",
                reason="stop_id_reached",
                stop_id=stop_id,
                last_id=last_id)
            # Não encerra — aguarda nova config via pvp-control
            idle_sleep(conn, poll_interval, stop_id, last_id)

        elif not messages:
            # Sem mensagens novas — idle até novo batch ou mudança de config
            log("idle", last_id=last_id, wait_s=poll_interval)
            idle_sleep(conn, poll_interval, stop_id, last_id)

        # Se messages > 0 e não atingiu stop_id → loop imediato (sem sleep)


if __name__ == "__main__":
    main()
