"""
PvP Control Panel — FastAPI web UI
Permite configurar START_ID, STOP_ID e POLL_INTERVAL do discord-producer em tempo real.
Auto-refresh a cada 10s. Lê stats do PostgreSQL.
"""

import os
import time as _time
import psycopg2
import docker as docker_sdk
from fastapi import FastAPI, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB",   "tabacaria_dw")
PG_USER = os.getenv("PG_USER", "tabacaria")
PG_PASS = os.getenv("PG_PASS", "tabacaria123")

DISCORD_EPOCH = 1420070400000  # ms — epoch do Discord Snowflake

app = FastAPI(title="PvP Control")


def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS,
    )


def snowflake_ms(sid: int) -> int:
    """Converte Discord Snowflake para Unix timestamp em ms."""
    return (sid >> 22) + DISCORD_EPOCH


def fmt_duration(ms: int) -> str:
    s = ms // 1000
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h {m:02d}m {sec:02d}s"
    return f"{m}m {sec:02d}s"


def get_container_logs(name: str, lines: int = 80) -> str:
    try:
        client = docker_sdk.from_env()
        container = client.containers.get(name)
        raw = container.logs(tail=lines, timestamps=False).decode("utf-8", errors="replace")
        return raw.strip() or "(sem saída)"
    except Exception as e:
        return f"(erro ao buscar logs de {name}: {e})"


def init_db():
    conn = get_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pvp_config (
                id            INTEGER PRIMARY KEY DEFAULT 1,
                start_id      BIGINT  DEFAULT 1479225035926540351,
                stop_id       BIGINT  DEFAULT 0,
                poll_interval INTEGER DEFAULT 30
            )
        """)
        cur.execute(
            "INSERT INTO pvp_config (id) VALUES (1) ON CONFLICT DO NOTHING"
        )
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


@app.get("/", response_class=HTMLResponse)
def index(msg: str = Query(default="")):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Config
            cur.execute("SELECT start_id, stop_id, poll_interval FROM pvp_config WHERE id = 1")
            row = cur.fetchone()
            start_id, stop_id, poll_interval = row if row else (0, 0, 30)

            # Cursor position
            cur.execute("SELECT last_msg_id FROM pvp_cursor WHERE id = 1")
            cr = cur.fetchone()
            last_msg_id = cr[0] if cr else "—"

            # Stats gerais
            cur.execute("SELECT COUNT(*) FROM pvp_stats")
            total_players = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM pvp_events")
            total_events = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT clan) FROM pvp_stats")
            total_clans = cur.fetchone()[0]

            # Duração da sessão via Snowflake
            cur.execute("SELECT MIN(msg_id), MAX(msg_id) FROM pvp_events")
            dur_row = cur.fetchone()
            min_id = dur_row[0] if dur_row and dur_row[0] else None
            max_id = dur_row[1] if dur_row and dur_row[1] else None

            # Kills por minuto (hora = HH:MM extraído da mensagem)
            cur.execute("""
                SELECT hora, COUNT(*) as kills
                FROM pvp_events
                GROUP BY hora
                ORDER BY hora
            """)
            minute_rows = cur.fetchall()

            # Per-clan kills
            cur.execute("""
                SELECT clan, SUM(kills) as k, SUM(deaths) as d, COUNT(*) as members
                FROM pvp_stats GROUP BY clan ORDER BY k DESC
            """)
            clan_rows = cur.fetchall()
    finally:
        conn.close()

    # Logs dos containers
    logs_producer = get_container_logs("discord-producer", lines=60)
    logs_spark    = get_container_logs("pvp-spark",        lines=60)

    # ── Duração ──────────────────────────────────────────────────────────────
    if min_id:
        start_ms = snowflake_ms(min_id)
        stopped = stop_id and last_msg_id != "—" and int(last_msg_id) >= stop_id - 1
        if stopped and max_id:
            end_ms = snowflake_ms(max_id)
        else:
            end_ms = int(_time.time() * 1000)
        duration_str = fmt_duration(end_ms - start_ms)
        duration_mode = "Encerrada" if stopped else "Em andamento"
    else:
        duration_str = "—"
        duration_mode = "Sem dados"

    # ── Banner ───────────────────────────────────────────────────────────────
    banners = {
        "restarted": ('<div class="banner banner-green">'
                      '✅ Cursor resetado para START_ID — producer vai buscar as mensagens em até 5s.'
                      '</div>'),
        "updated":   ('<div class="banner banner-blue">'
                      '✅ Configuração salva — cursor mantido na posição atual.'
                      '</div>'),
        "reset":     ('<div class="banner banner-red">'
                      '🗑 Todos os registros foram apagados. Dashboard zerado.'
                      '</div>'),
    }
    banner = banners.get(msg, "")

    def colorize_log(raw: str) -> str:
        import html as _html
        lines = []
        for line in raw.splitlines():
            escaped = _html.escape(line)
            lo = line.lower()
            if any(w in lo for w in ("error", "exception", "traceback", "errno", "failed", "critical")):
                lines.append(f'<span class="err">{escaped}</span>')
            elif any(w in lo for w in ("warn", "aviso", "retry")):
                lines.append(f'<span class="warn">{escaped}</span>')
            elif any(w in lo for w in ("kill", "enviado", "upsert", "batch", "commit", "published", "processado")):
                lines.append(f'<span class="ok">{escaped}</span>')
            else:
                lines.append(escaped)
        return "\n".join(lines)

    logs_producer_html = colorize_log(logs_producer)
    logs_spark_html    = colorize_log(logs_spark)

    stop_label = str(stop_id) if stop_id else "∞ (live)"
    mode_badge = (
        '<span style="background:#ef5350;color:#fff;padding:3px 10px;border-radius:12px;font-size:.8em">STOPPED</span>'
        if stop_id and last_msg_id != "—" and int(last_msg_id) >= stop_id - 1
        else '<span style="background:#66bb6a;color:#000;padding:3px 10px;border-radius:12px;font-size:.8em">LIVE</span>'
    )

    # ── Clan table ───────────────────────────────────────────────────────────
    clan_html = ""
    for clan, kills, deaths, members in clan_rows:
        kd = round(kills / deaths, 2) if deaths else "∞"
        clan_html += f"""
        <tr>
            <td>{clan}</td>
            <td style="color:#66bb6a">{kills}</td>
            <td style="color:#ef5350">{deaths}</td>
            <td>{kd}</td>
            <td>{members}</td>
        </tr>"""

    # ── Timelapse table ──────────────────────────────────────────────────────
    timelapse_html = ""
    peak_kills = max((k for _, k in minute_rows), default=0)
    for hora, kills in minute_rows:
        bar_w = int((kills / peak_kills) * 100) if peak_kills else 0
        timelapse_html += f"""
        <tr>
            <td style="font-family:monospace;color:#ffb74d">{hora}</td>
            <td style="color:#66bb6a;text-align:right;padding-right:12px">{kills}</td>
            <td style="width:60%">
                <div style="background:#66bb6a;height:10px;border-radius:3px;width:{bar_w}%"></div>
            </td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>⚔ PvP Control</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: 'Segoe UI', monospace;
      background: #0f0f1a;
      color: #d0d0e0;
      min-height: 100vh;
      padding: 30px 20px;
    }}
    .container {{ max-width: 780px; margin: 0 auto; }}
    h1 {{ color: #4fc3f7; font-size: 1.6em; margin-bottom: 6px; }}
    .subtitle {{ color: #666; font-size: .85em; margin-bottom: 28px; }}
    h2 {{ color: #81c784; font-size: 1em; text-transform: uppercase;
          letter-spacing: .1em; margin: 28px 0 14px; }}
    .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }}
    .stat-card {{
      background: #16213e; border: 1px solid #2a2a4a; border-radius: 10px;
      padding: 16px; text-align: center;
    }}
    .stat-val {{ font-size: 2em; font-weight: 700; color: #4fc3f7; line-height: 1.1; }}
    .stat-val.orange {{ color: #ffb74d; }}
    .stat-lbl {{ font-size: .75em; color: #888; margin-top: 4px; text-transform: uppercase; }}
    .stat-sub {{ font-size: .7em; color: #555; margin-top: 2px; }}
    .cursor-bar {{
      background: #16213e; border: 1px solid #2a2a4a; border-radius: 8px;
      padding: 12px 16px; margin-top: 12px;
      display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 8px;
    }}
    .cursor-bar .label {{ color: #888; font-size: .8em; text-transform: uppercase; }}
    .cursor-bar .val {{ color: #ffb74d; font-size: .9em; font-family: monospace; }}
    table {{ width: 100%; border-collapse: collapse; font-size: .9em; }}
    th {{ color: #888; font-size: .75em; text-transform: uppercase;
          padding: 8px 10px; text-align: left; border-bottom: 1px solid #2a2a4a; }}
    td {{ padding: 7px 10px; border-bottom: 1px solid #1a1a2e; }}
    tr:hover td {{ background: #1a1a2e; }}
    .form-card {{
      background: #16213e; border: 1px solid #2a2a4a; border-radius: 10px; padding: 24px;
    }}
    .danger-card {{
      background: #1e0f0f; border: 1px solid #5c2020; border-radius: 10px; padding: 20px;
    }}
    .field {{ margin-bottom: 16px; }}
    label {{ display: block; color: #9e9e9e; font-size: .82em; margin-bottom: 5px; }}
    input[type=number] {{
      width: 100%; background: #0f0f1a; border: 1px solid #3a3a5a;
      color: #e0e0e0; padding: 10px 14px; border-radius: 6px; font-size: 1em;
      transition: border-color .2s;
    }}
    input[type=number]:focus {{ outline: none; border-color: #4fc3f7; }}
    .hint {{ color: #555; font-size: .78em; margin-top: 4px; }}
    .btn-row {{ display: flex; gap: 10px; margin-top: 20px; flex-wrap: wrap; }}
    button {{
      flex: 1; padding: 11px; border: none; border-radius: 6px;
      font-size: .95em; font-weight: 600; cursor: pointer; transition: opacity .2s;
    }}
    button:hover {{ opacity: .85; }}
    .btn-apply     {{ background: #4fc3f7; color: #000; }}
    .btn-secondary {{ background: #37474f; color: #cfd8dc; }}
    .btn-danger    {{ background: #c62828; color: #fff; flex: none; width: 100%; margin-top: 12px; }}
    .banner {{
      padding: 12px 18px; border-radius: 8px; margin-bottom: 20px;
      font-size: .9em; font-weight: 500;
    }}
    .banner-green {{ background: #1b3a1f; border: 1px solid #66bb6a; color: #a5d6a7; }}
    .banner-blue  {{ background: #1a2a3a; border: 1px solid #4fc3f7; color: #81d4fa; }}
    .banner-red   {{ background: #3a1a1a; border: 1px solid #ef5350; color: #ef9a9a; }}
    .timelapse-wrap {{ max-height: 300px; overflow-y: auto; border-radius: 8px;
                       background: #16213e; border: 1px solid #2a2a4a; padding: 8px; }}
    .footer {{ color: #444; font-size: .78em; margin-top: 28px; text-align: center; }}
    .log-box {{
      background: #0a0a14; border: 1px solid #2a2a4a; border-radius: 8px;
      padding: 12px 14px; font-family: monospace; font-size: .78em;
      color: #b0b0c8; white-space: pre-wrap; word-break: break-all;
      max-height: 260px; overflow-y: auto;
    }}
    .log-box .ok  {{ color: #66bb6a; }}
    .log-box .err {{ color: #ef5350; }}
    .log-box .warn{{ color: #ffb74d; }}
    .log-tabs {{ display: flex; gap: 8px; margin-bottom: 8px; }}
    .log-tab {{
      padding: 5px 14px; border-radius: 6px 6px 0 0; border: 1px solid #2a2a4a;
      border-bottom: none; font-size: .8em; cursor: pointer; background: #16213e; color: #888;
    }}
    .log-tab.active {{ background: #0a0a14; color: #4fc3f7; }}
  </style>
</head>
<body>
<div class="container">
  {banner}
  <h1>⚔ PvP Control Panel</h1>
  <p class="subtitle">Auto-refresh: 10s &nbsp;|&nbsp; Producer: {mode_badge}</p>

  <h2>📊 Resumo</h2>
  <div class="stats">
    <div class="stat-card">
      <div class="stat-val">{total_players}</div>
      <div class="stat-lbl">Jogadores</div>
    </div>
    <div class="stat-card">
      <div class="stat-val">{total_events}</div>
      <div class="stat-lbl">Kill Events</div>
    </div>
    <div class="stat-card">
      <div class="stat-val">{total_clans}</div>
      <div class="stat-lbl">Clãs</div>
    </div>
    <div class="stat-card">
      <div class="stat-val orange">{duration_str}</div>
      <div class="stat-lbl">Duração</div>
      <div class="stat-sub">{duration_mode}</div>
    </div>
  </div>

  <div class="cursor-bar">
    <div>
      <div class="label">Último ID processado (cursor)</div>
      <div class="val">{last_msg_id}</div>
    </div>
    <div>
      <div class="label">STOP_ID atual</div>
      <div class="val">{stop_label}</div>
    </div>
    <div>
      <div class="label">Poll Interval</div>
      <div class="val">{poll_interval}s</div>
    </div>
  </div>

  <h2>🏆 Clãs</h2>
  <table>
    <thead>
      <tr><th>Clã</th><th>Kills</th><th>Deaths</th><th>K/D</th><th>Membros</th></tr>
    </thead>
    <tbody>{clan_html or '<tr><td colspan="5" style="color:#555;text-align:center">Sem dados ainda</td></tr>'}</tbody>
  </table>

  <h2>⏱ Timelapse — Kills por Minuto</h2>
  <div class="timelapse-wrap">
    <table>
      <thead>
        <tr><th>Minuto</th><th>Kills</th><th style="width:60%">Volume</th></tr>
      </thead>
      <tbody>{timelapse_html or '<tr><td colspan="3" style="color:#555;text-align:center;padding:12px">Sem dados ainda</td></tr>'}</tbody>
    </table>
  </div>

  <h2>⚙ Configuração do Producer</h2>
  <div class="form-card">
    <form method="post" action="/config">
      <div class="field">
        <label>START_ID — ponto de início ao resetar o cursor</label>
        <input type="number" name="start_id" value="{start_id}">
        <div class="hint">ID do Discord da primeira mensagem a processar</div>
      </div>
      <div class="field">
        <label>STOP_ID — 0 = modo live (não para nunca)</label>
        <input type="number" name="stop_id" value="{stop_id}">
        <div class="hint">0 = sem limite • qualquer valor > 0 para quando esse ID for atingido</div>
      </div>
      <div class="field">
        <label>Poll Interval (segundos)</label>
        <input type="number" name="poll_interval" value="{poll_interval}" min="5" max="300">
        <div class="hint">Intervalo de espera quando não há mensagens novas</div>
      </div>
      <div class="btn-row">
        <button type="submit" class="btn-apply" name="reset_cursor" value="1">
          ▶ Aplicar e iniciar do START_ID
        </button>
        <button type="submit" class="btn-secondary" name="reset_cursor" value="0">
          ⚙ Só atualizar config
        </button>
      </div>
      <p style="color:#555;font-size:.78em;margin-top:8px">
        <b>▶ Aplicar e iniciar</b> — salva config e reinicia o cursor do START_ID.<br>
        <b>⚙ Só atualizar config</b> — muda apenas STOP_ID / Interval, mantém cursor onde está.
      </p>
    </form>
  </div>

  <h2>🗑 Zona de Perigo</h2>
  <div class="danger-card">
    <p style="color:#ef9a9a;font-size:.88em;margin-bottom:14px">
      Apaga todos os registros de <b>pvp_stats</b> e <b>pvp_events</b> e reseta o cursor para o
      START_ID atual. O dashboard volta a zero. O producer reinicia a coleta imediatamente.
    </p>
    <form method="post" action="/reset"
          onsubmit="return confirm('⚠ Tem certeza? Todos os dados de kills/deaths serão apagados permanentemente.')">
      <button type="submit" class="btn-danger">🗑 Apagar todos os registros e reiniciar</button>
    </form>
  </div>

  <h2>🖥 Logs dos Containers</h2>
  <div class="log-tabs">
    <div class="log-tab active" onclick="showLog('producer',this)">discord-producer</div>
    <div class="log-tab"        onclick="showLog('spark',this)">pvp-spark</div>
  </div>
  <div id="log-producer" class="log-box">{logs_producer_html}</div>
  <div id="log-spark"    class="log-box" style="display:none">{logs_spark_html}</div>

  <p class="footer" style="margin-top:20px">
    Grafana → <a href="http://localhost:3000" style="color:#4fc3f7">localhost:3000</a>
    &nbsp;|&nbsp;
    Spark UI → <a href="http://localhost:4041" style="color:#4fc3f7">localhost:4041</a>
  </p>
</div>
<script>
function showLog(id, el) {{
  document.querySelectorAll('.log-box').forEach(b => b.style.display='none');
  document.querySelectorAll('.log-tab').forEach(t => t.classList.remove('active'));
  document.getElementById('log-' + id).style.display = 'block';
  el.classList.add('active');
}}
// Scroll logs to bottom on load
document.querySelectorAll('.log-box').forEach(b => b.scrollTop = b.scrollHeight);
</script>
</body>
</html>"""
    return HTMLResponse(html)


@app.post("/config")
def update_config(
    start_id:      int = Form(...),
    stop_id:       int = Form(...),
    poll_interval: int = Form(...),
    reset_cursor:  int = Form(default=0),
):
    conn = get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE pvp_config SET start_id=%s, stop_id=%s, poll_interval=%s WHERE id=1",
                (start_id, stop_id, poll_interval),
            )
            if reset_cursor:
                cur.execute(
                    "UPDATE pvp_cursor SET last_msg_id=%s WHERE id=1",
                    (start_id,),
                )
    finally:
        conn.close()
    if reset_cursor:
        return RedirectResponse("/?msg=restarted", status_code=303)
    return RedirectResponse("/?msg=updated", status_code=303)


@app.post("/reset")
def reset_all():
    """
    Reset completo da sessão:
    1. Trunca pvp_stats e pvp_events e reseta cursor
    2. Purga tópico Kafka pvp_kills (apaga mensagens antigas)
    3. Apaga checkpoint do Spark em /tmp via exec
    4. Reinicia pvp-spark
    """
    # 1. Limpa banco
    conn = get_conn()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pvp_events")
            cur.execute("TRUNCATE TABLE pvp_stats")
            cur.execute("""
                UPDATE pvp_cursor SET last_msg_id = (
                    SELECT start_id FROM pvp_config WHERE id = 1
                ) WHERE id = 1
            """)
    finally:
        conn.close()

    try:
        client = docker_sdk.from_env()

        # 2. Purga tópico Kafka — apaga todas as mensagens antigas
        kafka = client.containers.get("kafka")
        kafka.exec_run(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "
            "--delete --topic pvp_kills",
            detach=False
        )
        _time.sleep(2)
        kafka.exec_run(
            "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 "
            "--create --topic pvp_kills --partitions 1 --replication-factor 1",
            detach=False
        )

        # 3. Apaga checkpoint do Spark (docker restart NÃO limpa /tmp)
        spark = client.containers.get("pvp-spark")
        spark.exec_run("rm -rf /tmp/checkpoints/pvp", detach=False)

        # 4. Reinicia pvp-spark
        spark.restart(timeout=10)

    except Exception as e:
        print(f"[reset] aviso: {e}", flush=True)

    return RedirectResponse("/?msg=reset", status_code=303)
