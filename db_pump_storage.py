"""
db_pump_storage.py — PostgreSQL хранилище скринов псевдопампов
================================================================
Таблица pump_events:
  id              SERIAL PRIMARY KEY
  symbol          TEXT
  detected_at     TIMESTAMPTZ          — момент обнаружения
  pump_pct        REAL                 — % пампа
  pump_mins       INT                  — минут длился памп
  pump_speed_15   REAL                 — %/15мин скорость
  vol_ratio_pct   REAL                 — объём до/после пампа (%)
  oi_change_pct   REAL                 — изменение OI (%)
  crit_score      INT                  — кол-во совпавших критериев (0-4)
  peak_high       REAL                 — хай пампа
  pre_pump_low    REAL
  pre_pump_high   REAL
  retest_low      REAL
  retest_high     REAL
  natr            REAL
  chart_start     BYTEA                — PNG первого скрина (момент обнаружения)
  chart_end       BYTEA                — PNG последнего скрина (окончание)
  end_price       REAL                 — цена в момент окончания
  end_at          TIMESTAMPTZ          — когда зафиксировали конец
  outcome         TEXT                 — 'retest_short'/'prepump_long'/'expired'
  notes           TEXT                 — доп. заметки

Использование:
  from db_pump_storage import PumpStorage
  db = PumpStorage()          # подключается, создаёт таблицу если нет
  eid = db.save_start(symbol, details, chart_bytes)
  db.save_end(eid, end_price, chart_bytes, outcome)
  rows = db.query_patterns(min_pump_pct=20, max_oi_change=10)
"""

import os
import io
import time
from datetime import datetime

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_OK = True
except ImportError:
    PSYCOPG2_OK = False
    print("  [DB] psycopg2 не установлен. Запусти: pip install psycopg2-binary")


DATABASE_URL = os.environ.get("DATABASE_URL", "")   # Railway автоматически ставит эту переменную


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pump_events (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    pump_pct        REAL,
    pump_mins       INT,
    pump_speed_15   REAL,
    vol_ratio_pct   REAL,
    oi_change_pct   REAL,
    crit_score      INT,
    peak_high       REAL,
    pre_pump_low    REAL,
    pre_pump_high   REAL,
    retest_low      REAL,
    retest_high     REAL,
    natr            REAL,
    chart_start     BYTEA,
    chart_end       BYTEA,
    end_price       REAL,
    end_at          TIMESTAMPTZ,
    outcome         TEXT,
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_pump_symbol ON pump_events(symbol);
CREATE INDEX IF NOT EXISTS idx_pump_detected ON pump_events(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_pump_pct ON pump_events(pump_pct DESC);
"""


class PumpStorage:
    """Интерфейс к PostgreSQL для хранения скринов псевдопампов."""

    def __init__(self):
        self.conn = None
        if not PSYCOPG2_OK:
            print("  [DB] psycopg2 недоступен — хранилище отключено")
            return
        if not DATABASE_URL:
            print("  [DB] DATABASE_URL не задан — хранилище отключено")
            return
        self._connect()
        self._init_schema()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            self.conn.autocommit = True
            print("  [DB] ✅ PostgreSQL подключён")
        except Exception as e:
            print(f"  [DB] ❌ Ошибка подключения: {e}")
            self.conn = None

    def _ensure_conn(self):
        """Переподключается если соединение разорвано."""
        if self.conn is None:
            self._connect()
            return
        try:
            self.conn.cursor().execute("SELECT 1")
        except Exception:
            print("  [DB] Переподключение...")
            self._connect()

    def _init_schema(self):
        if not self.conn:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
            print("  [DB] Схема OK")
        except Exception as e:
            print(f"  [DB] Ошибка создания схемы: {e}")

    def save_start(self, symbol: str, details: dict, chart_bytes: bytes = None) -> int | None:
        """
        Сохраняет начальный скрин псевдопампа.
        Возвращает id записи (для последующего save_end) или None при ошибке.
        """
        self._ensure_conn()
        if not self.conn:
            return None
        try:
            sql = """
                INSERT INTO pump_events
                    (symbol, detected_at, pump_pct, pump_mins, pump_speed_15,
                     vol_ratio_pct, oi_change_pct, crit_score,
                     peak_high, pre_pump_low, pre_pump_high,
                     retest_low, retest_high, natr, chart_start)
                VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            vals = (
                symbol,
                details.get("pump_pct"),
                details.get("pump_mins"),
                details.get("pump_speed_15"),
                details.get("vol_ratio"),          # уже в % из details
                details.get("oi_change"),
                details.get("score"),
                details.get("peak_high"),
                details.get("pre_pump_low"),
                details.get("pre_pump_high"),
                details.get("retest_low"),
                details.get("retest_high"),
                details.get("natr"),
                psycopg2.Binary(chart_bytes) if chart_bytes else None,
            )
            with self.conn.cursor() as cur:
                cur.execute(sql, vals)
                row_id = cur.fetchone()[0]
            print(f"  [DB] 💾 {symbol} памп сохранён id={row_id}")
            return row_id
        except Exception as e:
            print(f"  [DB] save_start ERR: {e}")
            return None

    def save_end(self, event_id: int, end_price: float,
                 chart_bytes: bytes = None, outcome: str = "expired",
                 notes: str = "") -> bool:
        """
        Обновляет запись финальным скрином когда памп завершился.
        outcome: 'retest_short' | 'prepump_long' | 'expired'
        """
        self._ensure_conn()
        if not self.conn or event_id is None:
            return False
        try:
            sql = """
                UPDATE pump_events
                   SET end_price  = %s,
                       end_at     = NOW(),
                       chart_end  = %s,
                       outcome    = %s,
                       notes      = %s
                 WHERE id = %s
            """
            vals = (
                end_price,
                psycopg2.Binary(chart_bytes) if chart_bytes else None,
                outcome,
                notes,
                event_id,
            )
            with self.conn.cursor() as cur:
                cur.execute(sql, vals)
            print(f"  [DB] 📸 id={event_id} финал сохранён outcome={outcome}")
            return True
        except Exception as e:
            print(f"  [DB] save_end ERR: {e}")
            return False

    def query_patterns(self,
                       min_pump_pct: float = 0,
                       max_oi_change: float = 100,
                       min_crit_score: int = 3,
                       outcome: str = None,
                       limit: int = 50) -> list[dict]:
        """
        Поиск паттернов по параметрам.
        Пример: найти пампы >20% с OI < 10% и результатом 'retest_short':
          rows = db.query_patterns(min_pump_pct=20, max_oi_change=10, outcome='retest_short')
        """
        self._ensure_conn()
        if not self.conn:
            return []
        try:
            conditions = [
                "pump_pct >= %s",
                "oi_change_pct <= %s",
                "crit_score >= %s",
            ]
            params = [min_pump_pct, max_oi_change, min_crit_score]

            if outcome:
                conditions.append("outcome = %s")
                params.append(outcome)

            where = " AND ".join(conditions)
            sql   = f"""
                SELECT id, symbol, detected_at, pump_pct, pump_mins,
                       pump_speed_15, vol_ratio_pct, oi_change_pct,
                       crit_score, peak_high, outcome, notes,
                       end_price, end_at,
                       (end_price - peak_high) / NULLIF(peak_high, 0) * 100 AS drawdown_pct
                  FROM pump_events
                 WHERE {where}
                 ORDER BY detected_at DESC
                 LIMIT %s
            """
            params.append(limit)
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
        except Exception as e:
            print(f"  [DB] query_patterns ERR: {e}")
            return []

    def get_chart(self, event_id: int, which: str = "start") -> bytes | None:
        """
        Получает PNG байты скрина.
        which: 'start' | 'end'
        """
        self._ensure_conn()
        if not self.conn:
            return None
        col = "chart_start" if which == "start" else "chart_end"
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT {col} FROM pump_events WHERE id = %s", (event_id,))
                row = cur.fetchone()
                if row and row[0]:
                    return bytes(row[0])
            return None
        except Exception as e:
            print(f"  [DB] get_chart ERR: {e}")
            return None

    def stats_summary(self) -> str:
        """Возвращает текстовую сводку по всей базе (для Telegram)."""
        self._ensure_conn()
        if not self.conn:
            return "БД недоступна"
        try:
            sql = """
                SELECT
                    COUNT(*)                                        AS total,
                    ROUND(AVG(pump_pct)::numeric, 1)               AS avg_pump,
                    ROUND(AVG(pump_speed_15)::numeric, 1)          AS avg_speed,
                    SUM(CASE WHEN outcome='retest_short' THEN 1 ELSE 0 END) AS retest_cnt,
                    SUM(CASE WHEN outcome='prepump_long' THEN 1 ELSE 0 END) AS prepump_cnt,
                    SUM(CASE WHEN outcome='expired'      THEN 1 ELSE 0 END) AS expired_cnt,
                    ROUND(AVG(drawdown_pct)::numeric, 1)           AS avg_drawdown
                FROM (
                    SELECT pump_pct, pump_speed_15, outcome,
                           (end_price - peak_high) / NULLIF(peak_high,0) * 100 AS drawdown_pct
                      FROM pump_events
                     WHERE end_at IS NOT NULL
                ) sub
            """
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                r = dict(cur.fetchone())

            return (
                f"📦 <b>База псевдопампов</b>\n"
                f"Всего записей: <b>{r['total']}</b>\n"
                f"Средний памп: <b>{r['avg_pump']}%</b> · скорость {r['avg_speed']}%/15мин\n"
                f"Ретест шорт:  <b>{r['retest_cnt']}</b>\n"
                f"Pre-pump лонг: <b>{r['prepump_cnt']}</b>\n"
                f"Истекло:       <b>{r['expired_cnt']}</b>\n"
                f"Среднее падение от хая: <b>{r['avg_drawdown']}%</b>"
            )
        except Exception as e:
            return f"Ошибка запроса: {e}"

    def close(self):
        if self.conn:
            self.conn.close()


# ─────────────────────────────────────────────
#  Синглтон — один объект на весь процесс
# ─────────────────────────────────────────────
_db_instance: PumpStorage | None = None

def get_db() -> PumpStorage:
    global _db_instance
    if _db_instance is None:
        _db_instance = PumpStorage()
    return _db_instance
