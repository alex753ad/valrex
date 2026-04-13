"""
Crypto Screener v4 — 3-слойная архитектура
  Слой 1: Быстрое обнаружение объём-спайков (каждые 10 сек)
  Слой 2: Слежение за уровнями активных монет (каждые 15-30 сек)
  Слой 3: Оценка и алерт (мгновенно при касании уровня)
"""

import os
import requests
import time
import threading
import io
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Rectangle

# ─────────────────────────────────────────────
#  НАСТРОЙКИ
# ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "")

# Слой 1
LAYER1_INTERVAL     = 10       # сек между сканами
VOLUME_SPIKE_A      = 3.0      # триггер А: умеренный спайк
VOLUME_SPIKE_B      = 5.0      # триггер Б: сильный спайк
VOLUME_SPIKE_C      = 8.0      # триггер В: экстремальный спайк
ACTIVE_TTL          = 600      # сек — сколько монета остаётся активной

# Слой 2
LAYER2_INTERVAL_NORMAL = 30    # сек, режим NORMAL
LAYER2_INTERVAL_HOT    = 20    # сек, режим HOT
LAYER2_INTERVAL_WILD   = 5     # сек, режим WILD
MAX_TOUCHES         = 3        # касаний до сброса
FLAT_CANDLES        = 10       # свечей для определения флэта

# NATR-режимы
NATR_HOT            = 1.5      # % — переход в HOT
NATR_WILD           = 3.0      # % — переход в WILD

# Радиус близости к уровню (по режиму)
RADIUS_NORMAL       = 0.5      # %
RADIUS_HOT          = 0.8      # %
RADIUS_WILD         = 1.5      # %

# Фильтр ликвидности
MIN_VOL_5M_USD      = 100_000  # минимум $100k объёма за последние 5 мин
MIN_NATR            = 0.9      # минимальный NATR для наблюдения (%)

# Слой 3
MIN_STARS           = 2
ORDER_BOOK_DEPTH    = 20

MAX_WORKERS         = 20
BINANCE_BASE        = "https://fapi.binance.com"

# ─────────────────────────────────────────────
#  ГЛОБАЛЬНОЕ СОСТОЯНИЕ
# ─────────────────────────────────────────────

# { symbol: { "since": timestamp, "impulse_price": float,
#             "vol_mult": float, "touches": int,
#             "levels": [...], "last_alert": timestamp } }
active_coins: dict = {}
active_lock  = threading.Lock()

# Кэш последних алертов для дедупликации
alert_cache: dict  = {}
alert_lock   = threading.Lock()

proxy_lock   = threading.Lock()
current_proxy = {"http": None, "https": None}
PROXY_LIST: list = []

http_session = requests.Session()
http_session.headers.update({"User-Agent": "CryptoScreener/4.0"})

# ─────────────────────────────────────────────
#  ПРОКСИ
# ─────────────────────────────────────────────

def fetch_free_proxies():
    global PROXY_LIST
    sources = [
        "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
        "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    ]
    proxies = []
    for url in sources:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                for line in r.text.strip().split("\n"):
                    line = line.strip()
                    if line and ":" in line:
                        proxies.append(line)
        except Exception as e:
            print(f"  [PROXY FETCH ERR] {e}")
    random.shuffle(proxies)
    PROXY_LIST = proxies[:150]
    print(f"  [PROXY] Загружено: {len(PROXY_LIST)}")


def test_proxy(proxy_str):
    proxy = {"http": f"http://{proxy_str}", "https": f"http://{proxy_str}"}
    try:
        r = requests.get(f"{BINANCE_BASE}/fapi/v1/ping", proxies=proxy, timeout=6)
        return r.status_code == 200
    except Exception:
        return False


def find_working_proxy():
    global current_proxy
    print("  [PROXY] Ищу рабочий прокси...")
    for p in PROXY_LIST:
        if test_proxy(p):
            with proxy_lock:
                current_proxy = {"http": f"http://{p}", "https": f"http://{p}"}
            print(f"  [PROXY] ✅ {p}")
            return True
    print("  [PROXY] ❌ Не найден, работаю без прокси")
    with proxy_lock:
        current_proxy = {"http": None, "https": None}
    return False


def refresh_proxies():
    fetch_free_proxies()
    find_working_proxy()


# ─────────────────────────────────────────────
#  HTTP
# ─────────────────────────────────────────────

def fetch(url, params=None, timeout=8):
    with proxy_lock:
        proxy = dict(current_proxy)
    try:
        r = http_session.get(url, params=params, timeout=timeout, proxies=proxy)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 451:
            threading.Thread(target=find_working_proxy, daemon=True).start()
    except Exception as e:
        print(f"  [HTTP ERR] {e}")
    return None


# ─────────────────────────────────────────────
#  УТИЛИТЫ
# ─────────────────────────────────────────────

def fmt_vol(v):
    if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
    if v >= 1_000:     return f"{v/1_000:.1f}k"
    return str(int(v))

def fmt_usd(v):
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}k"
    return f"${v:.0f}"

def stars_str(n):
    n = max(0, min(5, int(n)))
    return "⭐" * n + "☆" * (5 - n)

def natr_mode(natr):
    if natr >= NATR_WILD:  return "WILD"
    if natr >= NATR_HOT:   return "HOT"
    return "NORMAL"

def radius_for_mode(mode):
    return {"NORMAL": RADIUS_NORMAL, "HOT": RADIUS_HOT, "WILD": RADIUS_WILD}[mode]

def layer2_interval(mode):
    return {"NORMAL": LAYER2_INTERVAL_NORMAL,
            "HOT":    LAYER2_INTERVAL_HOT,
            "WILD":   LAYER2_INTERVAL_WILD}[mode]


# ─────────────────────────────────────────────
#  ИНДИКАТОРЫ
# ─────────────────────────────────────────────

def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = float(candles[i][2]), float(candles[i][3]), float(candles[i-1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period

def calc_natr(candles):
    close = float(candles[-1][4])
    atr   = calc_atr(candles)
    return round(atr / close * 100, 2) if close else 0.0

def ema(data, period):
    k = 2 / (period + 1)
    result = [sum(data[:period]) / period]
    for p in data[period:]:
        result.append(p * k + result[-1] * (1 - k))
    return result

def get_trend(candles_15m):
    if not candles_15m or len(candles_15m) < 52:
        return "flat"
    closes = [float(c[4]) for c in candles_15m]
    e20 = ema(closes, 20)[-1]
    e50 = ema(closes, 50)[-1]
    c   = closes[-1]
    if c > e20 > e50: return "bull"
    if c < e20 < e50: return "bear"
    return "flat"

def calc_cvd(candles, lookback=20):
    data, running, cvd = candles[-lookback:], 0, []
    for c in data:
        o, cl, vol = float(c[1]), float(c[4]), float(c[5])
        running += vol if cl >= o else -vol
        cvd.append(running)
    if len(cvd) < 3: return "neutral"
    r = cvd[-3:]
    if r[-1] > r[-2] > r[-3]: return "up"
    if r[-1] < r[-2] < r[-3]: return "down"
    return "neutral"

def volume_mult(candles_5m, lookback=12):
    """Возвращает (последний_объём, множитель)"""
    if not candles_5m or len(candles_5m) < lookback + 2:
        return 0, 0
    recent = float(candles_5m[-2][7])
    avg    = sum(float(c[7]) for c in candles_5m[-lookback-2:-2]) / lookback
    return recent, round(recent / avg, 1) if avg > 0 else 0

def is_flat(candles_1m, n=FLAT_CANDLES):
    """Флэт: последние n свечей укладываются в диапазон < 0.5% от цены"""
    if len(candles_1m) < n:
        return False
    data   = candles_1m[-n:]
    highs  = [float(c[2]) for c in data]
    lows   = [float(c[3]) for c in data]
    rng    = max(highs) - min(lows)
    mid    = (max(highs) + min(lows)) / 2
    return (rng / mid * 100) < 0.5 if mid else False

def detect_pattern(candles):
    if len(candles) < 2: return None, None
    c, c1 = candles[-1], candles[-2]
    o,  h,  l,  cl  = float(c[1]),  float(c[2]),  float(c[3]),  float(c[4])
    o1, h1, l1, cl1 = float(c1[1]), float(c1[2]), float(c1[3]), float(c1[4])
    body     = abs(cl - o)
    full_rng = h - l if h != l else 0.0001
    us = h - max(o, cl)
    ls = min(o, cl) - l
    if ls >= body * 2 and ls >= us * 2:           return "🔨 Молот",              "bull"
    if us >= body * 2 and us >= ls * 2:           return "🔨 Перев. молот",       "bear"
    if body / full_rng < 0.1:                     return "➕ Доджи",              None
    if cl1 < o1 and cl > o and o <= cl1 and cl >= o1: return "🟢 Бычье поглощение", "bull"
    if cl1 > o1 and cl < o and o >= cl1 and cl <= o1: return "🔴 Медв. поглощение", "bear"
    if cl > o and body / full_rng > 0.7:          return "📗 Сильная бычья",      "bull"
    if cl < o and body / full_rng > 0.7:          return "📕 Сильная медвежья",   "bear"
    return None, None


# ─────────────────────────────────────────────
#  УРОВНИ
# ─────────────────────────────────────────────

def find_pivots(candles, tf_label, min_touches=2):
    highs = [float(c[2]) for c in candles]
    lows  = [float(c[3]) for c in candles]
    ph, pl = [], []
    wing = 2
    for i in range(wing, len(candles) - wing):
        if highs[i] >= max(highs[i-wing:i] + highs[i+1:i+wing+1]):
            ph.append(highs[i])
        if lows[i] <= min(lows[i-wing:i] + lows[i+1:i+wing+1]):
            pl.append(lows[i])

    def cluster(pts, pct=0.3):
        if not pts: return []
        pts = sorted(pts)
        groups, g = [], [pts[0]]
        for p in pts[1:]:
            if (p - g[0]) / g[0] * 100 < pct:
                g.append(p)
            else:
                groups.append(g); g = [p]
        groups.append(g)
        return [(sum(g)/len(g), len(g)) for g in groups if len(g) >= min_touches]

    levels = []
    for price, touches in cluster(ph):
        levels.append({"price": price, "touches": touches, "type": "res", "tf": tf_label})
    for price, touches in cluster(pl):
        levels.append({"price": price, "touches": touches, "type": "sup", "tf": tf_label})
    return levels

def collect_levels(symbol, k1):
    levels = find_pivots(k1[-100:], "1m")
    k5  = fetch(f"{BINANCE_BASE}/fapi/v1/klines", {"symbol": symbol, "interval": "5m",  "limit": 100})
    k15 = fetch(f"{BINANCE_BASE}/fapi/v1/klines", {"symbol": symbol, "interval": "15m", "limit": 100})
    if k5:  levels += find_pivots(k5,  "5m")
    if k15: levels += find_pivots(k15, "15m")
    return levels

def nearest_level(close, levels):
    if not levels: return None, None
    best = min(levels, key=lambda l: abs(l["price"] - close))
    return best, round(abs(best["price"] - close) / close * 100, 2)

def near_any_level(close, levels, radius_pct):
    for lv in levels:
        dist = abs(lv["price"] - close) / close * 100
        if dist <= radius_pct:
            return True, lv, round(dist, 2)
    return False, None, None


# ─────────────────────────────────────────────
#  СТАКАН
# ─────────────────────────────────────────────

def get_order_book_pressure(symbol, avg_vol):
    """Возвращает 'buy' / 'sell' / 'neutral' на основе стакана."""
    data = fetch(f"{BINANCE_BASE}/fapi/v1/depth",
                 {"symbol": symbol, "limit": ORDER_BOOK_DEPTH})
    if not data:
        return "neutral"
    bid_vol = sum(float(b[1]) for b in data.get("bids", []))
    ask_vol = sum(float(a[1]) for a in data.get("asks", []))
    total   = bid_vol + ask_vol
    if total == 0:
        return "neutral"
    ratio = bid_vol / total
    if ratio > 0.6:   return "buy"
    if ratio < 0.4:   return "sell"
    return "neutral"


# ─────────────────────────────────────────────
#  РЕЙТИНГ
# ─────────────────────────────────────────────

def calc_stars(near_level, pat_dir, sig_dir, trend, vol_mult_val,
               cvd, touch_num, ob_pressure, mode):
    score = 1.0
    if near_level:                                    score += 1.0
    if pat_dir and pat_dir == sig_dir:                score += 1.0
    if (sig_dir == "bull" and trend == "bull") or \
       (sig_dir == "bear" and trend == "bear"):       score += 0.5
    if vol_mult_val >= VOLUME_SPIKE_C:                score += 1.0
    elif vol_mult_val >= VOLUME_SPIKE_B:              score += 0.5
    if (sig_dir == "bull" and cvd == "up") or \
       (sig_dir == "bear" and cvd == "down"):         score += 0.5
    if touch_num == 2:                                score += 0.5   # 2е касание
    if touch_num >= 3:                                score += 1.0   # 3е касание
    if (sig_dir == "bull" and ob_pressure == "buy") or \
       (sig_dir == "bear" and ob_pressure == "sell"): score += 0.5
    if mode == "WILD":                                score += 0.5
    return min(5, round(score))


# ─────────────────────────────────────────────
#  TELEGRAM
# ─────────────────────────────────────────────

TF_COLORS = {
    "sup": {"1m": "#00e5ff", "5m": "#00ff99", "15m": "#ffff00"},
    "res": {"1m": "#ff4444", "5m": "#ff8800", "15m": "#ff44ff"},
}

def build_chart(symbol, candles, levels=None):
    try:
        data   = candles[-60:]
        n      = len(data)
        opens  = [float(c[1]) for c in data]
        highs  = [float(c[2]) for c in data]
        lows   = [float(c[3]) for c in data]
        closes = [float(c[4]) for c in data]
        dvols  = [float(c[7]) for c in data]
        times  = [datetime.utcfromtimestamp(int(c[0])/1000) for c in data]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8),
                                        gridspec_kw={"height_ratios": [3, 1]},
                                        facecolor="#0d1117")
        ax1.set_facecolor("#0d1117")
        ax2.set_facecolor("#0d1117")
        w = 0.6
        for i in range(n):
            color = "#26a69a" if closes[i] >= opens[i] else "#ef5350"
            ax1.plot([i, i], [lows[i], highs[i]], color=color, lw=0.8)
            bh = abs(closes[i]-opens[i]) or (highs[i]-lows[i])*0.01
            ax1.add_patch(Rectangle((i-w/2, min(opens[i], closes[i])), w, bh,
                                     facecolor=color, edgecolor=color))
            ax2.bar(i, dvols[i], color=color, width=w, alpha=0.85)

        if levels:
            pmn, pmx = min(lows), max(highs)
            margin = (pmx - pmn) * 0.15
            visible = [lv for lv in levels if pmn-margin <= lv["price"] <= pmx+margin]
            seen, deduped = [], []
            for lv in sorted(visible, key=lambda x: x["price"]):
                if not any(abs(lv["price"]-s)/s*100 < 0.2 for s in seen):
                    deduped.append(lv); seen.append(lv["price"])
            for lv in deduped[:12]:
                color = TF_COLORS.get(lv["type"], {}).get(lv["tf"], "#fff")
                lw = 1.5 if lv["tf"] == "15m" else 1.0
                ls = "-"  if lv["tf"] == "15m" else "--"
                ax1.axhline(y=lv["price"], color=color, lw=lw, ls=ls, alpha=0.9, zorder=5)
                ax1.text(n-0.5, lv["price"],
                         f" {lv['price']:.6g} [{lv['tf']}] ×{lv['touches']}",
                         color=color, fontsize=7, va="center", fontweight="bold",
                         bbox=dict(facecolor="#0d1117", alpha=0.6, pad=1, edgecolor="none"))

        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: fmt_usd(x)))
        ticks = list(range(0, n, max(1, n//6)))
        for ax in [ax1, ax2]:
            ax.set_xticks(ticks)
            ax.set_xticklabels([times[i].strftime("%H:%M") for i in ticks],
                                color="#8b949e", fontsize=8)
            ax.tick_params(colors="#8b949e", labelsize=8)
            ax.yaxis.set_tick_params(labelcolor="#8b949e")
            for sp in ax.spines.values(): sp.set_edgecolor("#30363d")
            ax.grid(color="#21262d", ls="--", lw=0.5)
            ax.set_xlim(-1, n)
        ax1.set_title(f"{symbol} · 1m", color="#e6edf3", fontsize=13, fontweight="bold", pad=8)
        ax1.set_ylabel("Price", color="#8b949e", fontsize=9)
        ax2.set_ylabel("Vol USD",  color="#8b949e", fontsize=9)
        plt.tight_layout(pad=1.2)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, facecolor="#0d1117")
        plt.close(fig); buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  [CHART ERR {symbol}] {e}")
        return None

def send_photo(image_bytes, caption):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        r = requests.post(url, data={"chat_id": TELEGRAM_CHANNEL_ID,
                                     "caption": caption, "parse_mode": "HTML"},
                          files={"photo": ("chart.png", image_bytes, "image/png")}, timeout=20)
        if not r.json().get("ok"):
            print(f"  [TG ERR] {r.json()}")
    except Exception as e:
        print(f"  [TG EXC] {e}")

def send_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHANNEL_ID,
                                     "text": text, "parse_mode": "HTML"}, timeout=10)
        if not r.json().get("ok"):
            print(f"  [TG ERR] {r.json()}")
    except Exception as e:
        print(f"  [TG EXC] {e}")


# ─────────────────────────────────────────────
#  ПОСТРОЕНИЕ СООБЩЕНИЙ
# ─────────────────────────────────────────────

def _trend_str(t):
    return {"bull": "▲ бычий", "bear": "▼ медвежий", "flat": "➡ флэт"}.get(t, "➡ флэт")

def _cvd_str(c):
    return {"up": "↑ покупатели активны", "down": "↓ продавцы активны",
            "neutral": "→ нейтрально"}.get(c, "→")

def _ob_str(o):
    return {"buy": "📗 перевес покупателей", "sell": "📕 перевес продавцов",
            "neutral": "⚖️ нейтрально"}.get(o, "⚖️")

def _mode_label(mode):
    return {"NORMAL": "нормальная волатильность",
            "HOT":    "повышенная волатильность",
            "WILD":   "ДИКАЯ ВОЛАТИЛЬНОСТЬ"}[mode]

def build_layer1_alert(symbol, vol_mult_val, trigger):
    """Быстрый алерт Слоя 1."""
    trigger_labels = {
        "A": f"объём {vol_mult_val:.1f}x",
        "B": f"🔥 сильный объём {vol_mult_val:.1f}x",
        "C": f"💥 экстремальный объём {vol_mult_val:.1f}x",
    }
    return (f"🔥 <b>{symbol}</b> активен · {trigger_labels.get(trigger, '')} · смотри\n"
            f"<i>Слой 1 — жду касания уровня...</i>")

def build_layer3_alert(symbol, close, level, dist_pct, natr, vol_mult_val,
                       cvd, pattern_name, trend, ob_pressure,
                       touch_num, stars, mode, candles_count):
    coin = symbol.replace("USDT", "")
    ltype = "поддержка" if level["type"] == "sup" else "сопротивление"

    if mode == "WILD":
        header = f"⚡️ WILD · <b>{symbol}</b> · {stars_str(stars)}"
        mode_line = f"Режим: <b>ДИКАЯ ВОЛАТИЛЬНОСТЬ</b> (NATR {natr}%)"
        warning = ("\n━━━━━━━━━━━━━━━━━━━\n"
                   f"⚠️ Высокий NATR: сквиз может быть 3–5%\n"
                   f"Ставь лимит у уровня, стакан обязателен")
    elif mode == "HOT":
        header = f"🔥 HOT · <b>{symbol}</b> · {stars_str(stars)}"
        mode_line = f"Режим: <b>повышенная волатильность</b> (NATR {natr}%)"
        warning = ""
    else:
        touch_label = {1: "1й вход", 2: "2е касание ✅", 3: "3е касание 🎯"}.get(touch_num, f"касание {touch_num}")
        header = f"📍 <b>{symbol}</b> · {touch_label} · {stars_str(stars)}"
        mode_line = f"Режим: нормальный (NATR {natr}%)"
        warning = ""

    squeeze_hint = "· сквизует к уровню" if dist_pct < radius_for_mode(mode) / 2 else ""

    lines = [
        header,
        "━━━━━━━━━━━━━━━━━━━",
        mode_line,
        f"Цена: <b>{close:.6g}</b> {squeeze_hint}",
        f"Уровень: <b>{level['price']:.6g}</b> [{level['tf']}] · расстояние: <b>{dist_pct}%</b> ({ltype})",
        f"Объём: <b>{vol_mult_val:.1f}x</b> от нормы",
        f"CVD: <b>{_cvd_str(cvd)}</b>",
        f"Стакан: <b>{_ob_str(ob_pressure)}</b>",
        f"Тренд 15m: <b>{_trend_str(trend)}</b>",
    ]
    if pattern_name:
        lines.append(f"Паттерн: <b>{pattern_name}</b>")
    lines.append(f"\n💎 <b>{coin}</b>")
    if warning:
        lines.append(warning)
    return "\n".join(lines)


# ─────────────────────────────────────────────
#  ДЕДУПЛИКАЦИЯ АЛЕРТОВ
# ─────────────────────────────────────────────

def can_alert(symbol, touch_num, level_price=0, cooldown=60):
    """
    Дедупликация: один и тот же символ + уровень не шлётся чаще cooldown сек.
    При новом касании (touch_num меняется) — пропускаем сразу.
    """
    now = time.time()
    # Ключ: символ + округлённая цена уровня + номер касания
    key = f"{symbol}_{round(level_price, 6)}_{touch_num}"
    with alert_lock:
        last = alert_cache.get(key, 0)
        if now - last >= cooldown:
            alert_cache[key] = now
            return True
    return False


# ─────────────────────────────────────────────
#  СЛОЙ 1 — БЫСТРОЕ ОБНАРУЖЕНИЕ
# ─────────────────────────────────────────────

def get_all_symbols():
    data = fetch(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    if not data:
        return []
    return [s["symbol"] for s in data["symbols"]
            if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"]

def layer1_scan_symbol(symbol):
    """Проверяет один символ на объём-спайк. Возвращает кортеж с данными пробойной свечи или None."""
    k5 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "5m", "limit": 16})
    if not k5 or len(k5) < 14:
        return None
    recent, mult = volume_mult(k5, lookback=12)

    # Фильтр ликвидности: минимум $100k за последние 5 мин
    if recent < MIN_VOL_5M_USD:
        return None

    if mult < VOLUME_SPIKE_A:
        return None

    # Проверяем NATR по последним 1m свечам
    k1_check = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                     {"symbol": symbol, "interval": "1m", "limit": 20})
    if k1_check and len(k1_check) >= 15:
        natr_check = calc_natr(k1_check)
        if natr_check < MIN_NATR:
            return None  # монета слишком вялая

    # Пробойная свеча = k5[-2] (закрытая, та что дала спайк)
    spike_candle  = k5[-2]
    imp_close     = float(spike_candle[4])   # close пробойной
    imp_high      = float(spike_candle[2])   # high  пробойной
    imp_low       = float(spike_candle[3])   # low   пробойной
    current_close = float(k5[-1][4])         # текущая цена

    trigger = "C" if mult >= VOLUME_SPIKE_C else \
              "B" if mult >= VOLUME_SPIKE_B else "A"

    return (trigger, mult, k5, recent,
            imp_close, imp_high, imp_low, current_close)

def layer1_loop(symbols_ref):
    """Бесконечный цикл Слоя 1."""
    print("  [L1] Слой 1 запущен")
    while True:
        start = time.time()
        symbols = symbols_ref[0]
        if not symbols:
            time.sleep(LAYER1_INTERVAL)
            continue

        def scan_one(sym):
            result = layer1_scan_symbol(sym)
            if result is None:
                return
            trigger, mult, k5, recent_vol, \
                imp_close, imp_high, imp_low, cur_close = result

            with active_lock:
                already_active = sym in active_coins
                active_coins[sym] = {
                    "since":          time.time(),
                    "impulse_close":  imp_close,
                    "impulse_high":   imp_high,
                    "impulse_low":    imp_low,
                    "impulse_price":  cur_close,
                    "vol_mult":       mult,
                    "touches":        active_coins.get(sym, {}).get("touches", 0),
                    "levels":         active_coins.get(sym, {}).get("levels", []),
                    "last_level_scan": 0,
                    "last_natr":      2.0,
                    "pullback_high":  0,
                }

            if not already_active:
                msg = build_layer1_alert(sym, mult, trigger)
                send_message(msg)
                print(f"  [L1] 🔥 {sym} | {trigger} | {mult:.1f}x | imp_close={imp_close:.6g}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            list(pool.map(scan_one, symbols))

        # Удаляем протухшие монеты
        now = time.time()
        with active_lock:
            expired = [s for s, v in active_coins.items()
                       if now - v["since"] > ACTIVE_TTL]
            for s in expired:
                del active_coins[s]
                print(f"  [L1] ⏰ {s} убран из активных")

        elapsed = time.time() - start
        time.sleep(max(0, LAYER1_INTERVAL - elapsed))


# ─────────────────────────────────────────────
#  СЛОЙ 2 — СЛЕЖЕНИЕ ЗА УРОВНЯМИ
# ─────────────────────────────────────────────

def get_price_fast(symbol):
    """Только текущая цена — один лёгкий запрос."""
    data = fetch(f"{BINANCE_BASE}/fapi/v1/ticker/price", {"symbol": symbol})
    return float(data["price"]) if data else None


def layer2_watch_symbol_fast(symbol, state):
    """
    Быстрая проверка для WILD монет.
    Грузит только цену, свечи — только при обнаружении касания.
    """
    close = get_price_fast(symbol)
    if not close:
        return
    levels = state.get("levels", [])
    if not levels:
        return
    natr   = state.get("last_natr", 2.0)
    # Живой фильтр NATR из кэша
    if natr < MIN_NATR:
        with active_lock:
            if symbol in active_coins:
                del active_coins[symbol]
        print(f"  [L2] ⚡ {symbol} убран (fast): natr={natr:.2f}%")
        return
    mode   = natr_mode(natr)
    radius = radius_for_mode(mode)
    near, level, dist_pct = near_any_level(close, levels, radius)
    if not near:
        return
    # Касание найдено — грузим свечи для оценки
    k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "1m", "limit": 60})
    if not k1:
        return
    flat = is_flat(k1)
    layer3_evaluate(symbol, close, level, dist_pct, natr, mode, state, k1, flat)


def detect_pullback(symbol, state, k1):
    """
    Детектор начала отката от хая к уровню.
    Шлёт алерт ДО касания — чтобы успеть поставить лимит.
    """
    if len(k1) < 10:
        return
    closes = [float(c[4]) for c in k1]
    highs  = [float(c[2]) for c in k1]
    close  = closes[-1]
    natr   = calc_natr(k1)

    recent_high = max(highs[-10:])
    drop_pct    = (recent_high - close) / recent_high * 100

    # Откат от хая >= 0.5 * NATR
    if drop_pct < natr * 0.5:
        return

    # Последние 2 свечи идут вниз
    if not (closes[-1] < closes[-2] and closes[-2] < closes[-3]):
        return

    # Есть уровень поддержки ниже текущей цены
    levels = state.get("levels", [])
    sup_levels = [lv for lv in levels if lv["type"] == "sup" and lv["price"] < close]
    if not sup_levels:
        return

    nearest    = max(sup_levels, key=lambda x: x["price"])
    dist_to_lv = (close - nearest["price"]) / close * 100

    # Уровень в диапазоне 0.3% – 3*NATR
    if not (0.3 <= dist_to_lv <= natr * 3):
        return

    # Дедупликация по хаю
    last_high = state.get("pullback_high", 0)
    if last_high > 0 and abs(recent_high - last_high) / recent_high * 100 < 0.3:
        return

    with active_lock:
        if symbol in active_coins:
            active_coins[symbol]["pullback_high"] = recent_high

    msg = (
        f"📉 <b>{symbol}</b> — начало отката от хая\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"Хай: <b>{recent_high:.6g}</b> → цена: <b>{close:.6g}</b> (−{drop_pct:.1f}%)\n"
        f"Идёт к уровню: <b>{nearest['price']:.6g}</b> [{nearest['tf']}]\n"
        f"Расстояние до уровня: <b>{dist_to_lv:.2f}%</b>\n"
        f"NATR: {natr:.2f}% · режим: {natr_mode(natr)}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Готовь лимит у уровня · следующий алерт при касании"
    )
    send_message(msg)
    print(f"  [PB] 📉 {symbol} откат {drop_pct:.1f}% → уровень {nearest['price']:.6g} ({dist_to_lv:.2f}%)")


def layer2_watch_symbol(symbol, state):
    """Обновляет уровни и проверяет касание для одной монеты."""
    k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "1m", "limit": 120})
    if not k1 or len(k1) < 20:
        return

    close = float(k1[-1][4])
    natr  = calc_natr(k1)
    mode  = natr_mode(natr)
    radius = radius_for_mode(mode)

    # ── Живые фильтры: проверяем объём и NATR прямо сейчас ──
    k5_live = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                    {"symbol": symbol, "interval": "5m", "limit": 16})
    if k5_live:
        live_vol, live_mult = volume_mult(k5_live, lookback=12)
        # Если объём упал ниже $100k или NATR ниже порога — убираем из активных
        if live_vol < MIN_VOL_5M_USD or natr < MIN_NATR:
            with active_lock:
                if symbol in active_coins:
                    del active_coins[symbol]
            print(f"  [L2] ⚡ {symbol} убран: vol={live_vol:.0f} natr={natr:.2f}%")
            return
    else:
        live_mult = state.get("vol_mult", 1.0)

    # Кэшируем NATR для быстрого поллинга
    with active_lock:
        if symbol in active_coins:
            active_coins[symbol]["last_natr"] = natr
            active_coins[symbol]["vol_mult"]  = live_mult

    # Обновляем уровни раз в 2 минуты
    now = time.time()
    if now - state.get("last_level_scan", 0) > 120:
        levels = collect_levels(symbol, k1)
        # Три уровня от пробойной свечи Слоя 1
        impulse_levels = [
            (state.get("impulse_close"), "sup", "impulse"),
            (state.get("impulse_high"),  "res", "imp-high"),
            (state.get("impulse_low"),   "sup", "imp-low"),
        ]
        for price, ltype, label in impulse_levels:
            if price:
                levels.append({"price": price, "touches": 1, "type": ltype, "tf": label})
        state["levels"] = levels
        state["last_level_scan"] = now
    else:
        levels = state.get("levels", [])

    # Флэт-детектор
    flat = is_flat(k1)

    # Детектор отката — шлём алерт ДО касания уровня
    detect_pullback(symbol, state, k1)

    # Проверяем близость к уровню
    near, level, dist_pct = near_any_level(close, levels, radius)
    if not near:
        return

    # Передаём в Слой 3
    layer3_evaluate(symbol, close, level, dist_pct, natr, mode,
                    state, k1, flat)

def layer2_loop():
    """Бесконечный цикл Слоя 2."""
    print("  [L2] Слой 2 запущен")
    while True:
        with active_lock:
            snapshot = dict(active_coins)

        if not snapshot:
            time.sleep(5)
            continue

        for symbol, state in snapshot.items():
            # Режим из кэшированного NATR (без лишних запросов)
            natr_q = state.get("last_natr", 1.0)
            mode_q = natr_mode(natr_q)

            # WILD: быстрый поллинг только цены
            if mode_q == "WILD":
                interval = LAYER2_INTERVAL_WILD
                watch_fn = layer2_watch_symbol_fast
            else:
                interval = layer2_interval(mode_q)
                watch_fn = layer2_watch_symbol

            last_scan = state.get("last_l2_scan", 0)
            if time.time() - last_scan < interval:
                continue

            with active_lock:
                if symbol in active_coins:
                    active_coins[symbol]["last_l2_scan"] = time.time()

            try:
                watch_fn(symbol, state)
            except Exception as e:
                print(f"  [L2 ERR {symbol}] {e}")

        time.sleep(1)


# ─────────────────────────────────────────────
#  СЛОЙ 3 — ОЦЕНКА И АЛЕРТ
# ─────────────────────────────────────────────

def layer3_evaluate(symbol, close, level, dist_pct, natr, mode,
                    state, k1, flat):
    """Мгновенная оценка сигнала и отправка алерта."""

    # Считаем касания
    touch_num = state.get("touches", 0) + 1

    # Дедупликация: один символ + один уровень не дублируется чаще 60 сек
    if not can_alert(symbol, touch_num, level_price=level["price"]):
        return

    # Определяем направление
    sig_dir = "bull" if level["type"] == "sup" else "bear"

    # Паттерн
    pattern_name, pat_dir = detect_pattern(k1)

    # Тренд 15m
    k15 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                {"symbol": symbol, "interval": "15m", "limit": 55})
    trend = get_trend(k15) if k15 else "flat"

    # CVD
    cvd = calc_cvd(k1)

    # Стакан (последним, как указано)
    k5 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "5m", "limit": 16})
    _, vol_mult_val = volume_mult(k5) if k5 else (0, state.get("vol_mult", 1))
    vol_mult_val = vol_mult_val or state.get("vol_mult", 1)

    ob_pressure = get_order_book_pressure(symbol, vol_mult_val)

    # Рейтинг
    stars = calc_stars(True, pat_dir, sig_dir, trend, vol_mult_val,
                       cvd, touch_num, ob_pressure, mode)

    if stars < MIN_STARS:
        return

    # Обновляем касания
    with active_lock:
        if symbol in active_coins:
            active_coins[symbol]["touches"] = touch_num
            # Пробой уровня — сброс касаний
            if touch_num >= MAX_TOUCHES:
                active_coins[symbol]["touches"] = 0

    # Строим алерт
    caption = build_layer3_alert(
        symbol, close, level, dist_pct, natr, vol_mult_val,
        cvd, pattern_name, trend, ob_pressure,
        touch_num, stars, mode, len(k1)
    )

    print(f"  [L3] 🎯 {symbol} | {mode} | касание {touch_num} | ⭐{stars} | dist={dist_pct}%")

    chart = build_chart(symbol, k1, state.get("levels", []))
    if chart:
        send_photo(chart, caption)
    else:
        send_message(caption)


# ─────────────────────────────────────────────
#  ГЛАВНЫЙ ЗАПУСК
# ─────────────────────────────────────────────

def main():
    print("🚀 Crypto Screener v4 — 3-слойная архитектура\n")

    # Прокси
    refresh_proxies()
    threading.Thread(target=lambda: [time.sleep(1800) or refresh_proxies()
                                     for _ in iter(int, 1)], daemon=True).start()

    # Список символов (обновляется каждые 5 мин)
    symbols_ref = [[]]
    def refresh_symbols():
        while True:
            s = get_all_symbols()
            if s:
                symbols_ref[0] = s
                print(f"  [SYM] Символов: {len(s)}")
            time.sleep(300)
    threading.Thread(target=refresh_symbols, daemon=True).start()
    # Первая загрузка
    time.sleep(3)
    symbols_ref[0] = get_all_symbols()
    print(f"  [SYM] Символов загружено: {len(symbols_ref[0])}")

    # Слой 2 в отдельном потоке
    threading.Thread(target=layer2_loop, daemon=True).start()

    # Слой 1 — основной поток
    layer1_loop(symbols_ref)


if __name__ == "__main__":
    main()
