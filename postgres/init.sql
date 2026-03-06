-- ═══════════════════════════════════════════════════════
--  Tabacaria DW — live feed para o Grafana
--  Populado pelo Spark pipeline (camada silver)
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS live_feed (
    id             BIGSERIAL,
    transaction_id TEXT,
    event_time     TIMESTAMP     NOT NULL,
    customer_id    TEXT,
    product_name   TEXT,
    category       TEXT,
    brand          TEXT,
    quantity       INTEGER,
    unit_price     NUMERIC(10,2),
    total_amount   NUMERIC(10,2),
    payment_method TEXT,
    payment_status TEXT,
    state          TEXT,
    device         TEXT,
    processed_at   TIMESTAMP     DEFAULT NOW()
);

-- Índice principal para o Grafana (queries DESC por horário)
CREATE INDEX IF NOT EXISTS idx_live_feed_event_time ON live_feed (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_live_feed_status     ON live_feed (payment_status);

-- ═══════════════════════════════════════════════════════
--  PvP — Dados de batalha (Discord scraper via Kafka)
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pvp_stats (
    nick              TEXT PRIMARY KEY,
    clan              TEXT,
    kills             INTEGER      DEFAULT 0,
    deaths            INTEGER      DEFAULT 0,
    kd                NUMERIC(6,2) DEFAULT 0,
    kill_streak       INTEGER      DEFAULT 0,   -- máximo histórico
    death_streak      INTEGER      DEFAULT 0,   -- máximo histórico
    cur_kill_streak   INTEGER      DEFAULT 0,   -- streak ativa agora
    cur_death_streak  INTEGER      DEFAULT 0    -- streak ativa agora
);

CREATE TABLE IF NOT EXISTS pvp_events (
    msg_id      BIGINT,
    hora        TEXT,
    cla_kill    TEXT,
    nick_kill   TEXT,
    cla_death   TEXT,
    nick_death  TEXT,
    UNIQUE (msg_id, nick_kill, nick_death, hora)
);

CREATE TABLE IF NOT EXISTS pvp_cursor (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    last_msg_id BIGINT  DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pvp_stats_clan  ON pvp_stats (clan);
CREATE INDEX IF NOT EXISTS idx_pvp_events_kill ON pvp_events (nick_kill);
CREATE INDEX IF NOT EXISTS idx_pvp_events_death ON pvp_events (nick_death);
