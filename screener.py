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
ACTIVE_TTL          = 1800     # сек — 30 мин монета остаётся активной
MIN_VOL_SPIKE_ABS   = 200_000  # минимум $200k абсолютного объёма пробойной свечи

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

# Инплей модуль
INPLAY_PUMP_PCT     = 10.0     # % роста от лоя за 30 мин
INPLAY_PUMP_MINS    = 30       # окно поиска лоя
INPLAY_NATR_MIN     = 1.0      # минимальный NATR для инплей
INPLAY_VOL_MULT     = 3.0      # минимальный спайк объёма
INPLAY_SCAN_SEC     = 20       # интервал сканирования инплей
INPLAY_TTL          = 3600     # 1 час монета в инплей-наблюдении
INPLAY_HOLD_CANDLES = 1        # мин закрытых 5m свечей выше уровня
ZONE_APPROACH_PCT   = 2.0      # % от верхней границы зоны → алерт "идёт к зоне"
ORDER_BOOK_DEPTH    = 20

# Детектор псевдопампа
PSEUDO_VOL_RATIO    = 0.05     # объём до пампа < 5% от объёма пампа → псевдопамп
PSEUDO_PUMP_SPEED   = 15.0     # % за 15 минут — порог скорости псевдопампа
PSEUDO_OI_CHANGE    = 15.0     # % — если OI изменился меньше, это псевдопамп
PSEUDO_TTL          = 3600     # 1 час монета в псевдопамп-наблюдении
PSEUDO_SCAN_SEC     = 20       # интервал сканирования псевдопампов

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

# Инплей
inplay_coins: dict = {}
inplay_lock   = threading.Lock()
inplay_alert_cache: dict = {}

# Псевдопамп
pseudo_coins: dict = {}
pseudo_lock   = threading.Lock()
pseudo_alert_cache: dict = {}

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

def calc_vwap(candles):
    """Рассчитывает VWAP по свечам (типичная цена * объём / сумма объёмов)."""
    vwap = []
    cum_vol = 0.0
    cum_pv  = 0.0
    for c in candles:
        typ_price = (float(c[2]) + float(c[3]) + float(c[4])) / 3
        vol       = float(c[5])
        cum_pv   += typ_price * vol
        cum_vol  += vol
        vwap.append(cum_pv / cum_vol if cum_vol > 0 else typ_price)
    return vwap


def draw_volume_profile(ax, candles, n_bins=40, alpha=0.25, poc_color="#ffd700"):
    """
    Рисует горизонтальный Volume Profile (распределение объёма по цене) 
    справа от графика, как на TradingView.
    poc_color — цвет линии POC (Point of Control, максимальный объём).
    """
    if not candles:
        return

    highs  = [float(c[2]) for c in candles]
    lows   = [float(c[3]) for c in candles]
    vols   = [float(c[7]) for c in candles]  # quote volume (USD)

    price_min = min(lows)
    price_max = max(highs)
    if price_max <= price_min:
        return

    # Разбиваем диапазон цен на n_bins бинов
    bin_size  = (price_max - price_min) / n_bins
    bin_vols  = [0.0] * n_bins

    for i, c in enumerate(candles):
        h   = float(c[2])
        l   = float(c[3])
        vol = vols[i]
        # Распределяем объём свечи по всем бинам которые она перекрывает
        for b in range(n_bins):
            bin_lo = price_min + b * bin_size
            bin_hi = bin_lo + bin_size
            overlap = max(0, min(h, bin_hi) - max(l, bin_lo))
            candle_range = h - l if h > l else bin_size
            bin_vols[b] += vol * overlap / candle_range

    max_vol = max(bin_vols) if max(bin_vols) > 0 else 1

    # Находим POC — бин с максимальным объёмом
    poc_bin   = bin_vols.index(max_vol)
    poc_price = price_min + (poc_bin + 0.5) * bin_size

    # Получаем текущие xlim чтобы нарисовать профиль справа
    xlim = ax.get_xlim()
    x_range   = xlim[1] - xlim[0]
    bar_width  = x_range * 0.12  # профиль занимает 12% ширины графика
    x_right    = xlim[1]          # правый край

    for b in range(n_bins):
        bin_lo    = price_min + b * bin_size
        bin_price = bin_lo + bin_size / 2
        vol_norm  = bin_vols[b] / max_vol
        bar_len   = bar_width * vol_norm

        # Цвет: бины выше POC — зеленоватые, ниже — красноватые
        if b > poc_bin:
            color = "#26a69a"
        elif b < poc_bin:
            color = "#ef5350"
        else:
            color = poc_color  # POC — золотой

        ax.barh(bin_price, bar_len, height=bin_size * 0.85,
                left=x_right - bar_len,
                color=color, alpha=alpha, zorder=3)

    # Линия POC
    ax.axhline(y=poc_price, color=poc_color, lw=1.0, ls="-",
               alpha=0.8, zorder=5)
    ax.text(x_right, poc_price,
            f" POC {poc_price:.6g}",
            color=poc_color, fontsize=6, va="center",
            fontweight="bold", zorder=7,
            bbox=dict(facecolor="#0d1117", alpha=0.7, pad=1, edgecolor="none"))


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

        # VWAP
        vwap_vals = calc_vwap(data)
        ax1.plot(range(n), vwap_vals, color="#ff9800", lw=1.2,
                 ls="--", alpha=0.85, label="VWAP", zorder=6)
        ax1.legend(loc="upper left", fontsize=7, facecolor="#161b22",
                   edgecolor="#30363d", labelcolor="white", framealpha=0.7)

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

        # Volume Profile (после установки xlim)
        draw_volume_profile(ax1, data)

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
                       last_1m_vol_usd, cvd, pattern_name, trend, ob_pressure,
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
        f"Объём 1m: <b>{fmt_usd(last_1m_vol_usd)}</b> · спайк: <b>{vol_mult_val:.1f}x</b> от нормы",
        f"CVD: <b>{_cvd_str(cvd)}</b>",
        f"Стакан: <b>{_ob_str(ob_pressure)}</b>",
        f"Тренд 15m: <b>{_trend_str(trend)}</b>",
    ]
    if pattern_name:
        lines.append(f"Паттерн: <b>{pattern_name}</b>")
    lines.append(f"\n💎 <code>{coin}</code>")
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
    spike_vol_usd = float(spike_candle[7])   # объём пробойной свечи в USD
    current_close = float(k5[-1][4])         # текущая цена

    # Абсолютный объём пробойной свечи >= $200k
    if spike_vol_usd < MIN_VOL_SPIKE_ABS:
        return None

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
                # Слой 1 работает молча — только добавляет монету в наблюдение
                # Уведомление придёт только от Слоя 3 при реальном касании уровня
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
    Детектор начала движения к уровню.
    Различает:
    - откат от хая (памп → ищет поддержку)
    - отскок от лоя (дамп → ищет сопротивление)
    """
    if len(k1) < 10:
        return
    closes = [float(c[4]) for c in k1]
    highs  = [float(c[2]) for c in k1]
    lows   = [float(c[3]) for c in k1]
    close  = closes[-1]
    natr   = calc_natr(k1)
    levels = state.get("levels", [])

    # ── ОТКАТ ОТ ХАЯ (памп → цена падает к поддержке) ──
    recent_high = max(highs[-10:])
    drop_pct    = (recent_high - close) / recent_high * 100
    going_down  = closes[-1] < closes[-2] and closes[-2] < closes[-3]

    if drop_pct >= natr * 0.5 and going_down:
        sup_levels = [lv for lv in levels if lv["type"] == "sup" and lv["price"] < close]
        if sup_levels:
            nearest    = max(sup_levels, key=lambda x: x["price"])
            dist_to_lv = (close - nearest["price"]) / close * 100
            if 0.3 <= dist_to_lv <= natr * 3:
                last_high = state.get("pullback_high", 0)
                if last_high == 0 or abs(recent_high - last_high) / recent_high * 100 >= 0.3:
                    with active_lock:
                        if symbol in active_coins:
                            active_coins[symbol]["pullback_high"] = recent_high
                    # Откат отслеживается внутренне — уведомление придёт при касании уровня
                    print(f"  [PB] 📉 {symbol} откат {drop_pct:.1f}% → поддержка {nearest['price']:.6g}")

    if rise_pct >= natr * 0.5 and going_up:
        res_levels = [lv for lv in levels if lv["type"] == "res" and lv["price"] > close]
        if res_levels:
            nearest    = min(res_levels, key=lambda x: x["price"])
            dist_to_lv = (nearest["price"] - close) / close * 100
            if 0.3 <= dist_to_lv <= natr * 3:
                last_low = state.get("pullback_low", 0)
                if last_low == 0 or abs(recent_low - last_low) / recent_low * 100 >= 0.3:
                    with active_lock:
                        if symbol in active_coins:
                            active_coins[symbol]["pullback_low"] = recent_low
                    # Отскок отслеживается внутренне — уведомление придёт при касании уровня
                    print(f"  [PB] 📈 {symbol} отскок {rise_pct:.1f}% → сопротивление {nearest['price']:.6g}")


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

    # Жёсткие фильтры прямо в Слое 3
    if natr < MIN_NATR:
        return

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

    # Фильтр: тренд флэт — не торгуем
    if trend == "flat":
        return

    # CVD
    cvd = calc_cvd(k1)

    # Объём за последнюю 1m свечу (в USD)
    last_1m_vol_usd = float(k1[-1][7]) if k1 else 0

    # Фильтр: объём за последнюю минуту < $100k — пропускаем
    if last_1m_vol_usd < MIN_VOL_5M_USD:
        return

    # Стакан (последним)
    k5 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "5m", "limit": 16})
    live_vol_usd, vol_mult_val = volume_mult(k5) if k5 else (0, state.get("vol_mult", 1))
    vol_mult_val = vol_mult_val or state.get("vol_mult", 1)

    # Фильтр объёма за 5m
    if live_vol_usd < MIN_VOL_5M_USD:
        return

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
            if touch_num >= MAX_TOUCHES:
                active_coins[symbol]["touches"] = 0

    # Строим алерт
    caption = build_layer3_alert(
        symbol, close, level, dist_pct, natr, vol_mult_val,
        last_1m_vol_usd, cvd, pattern_name, trend, ob_pressure,
        touch_num, stars, mode, len(k1)
    )

    print(f"  [L3] 🎯 {symbol} | {mode} | касание {touch_num} | ⭐{stars} | dist={dist_pct}%")

    chart = build_chart(symbol, k1, state.get("levels", []))
    if chart:
        send_photo(chart, caption)
    else:
        send_message(caption)


# ─────────────────────────────────────────────
#  ИНПЛЕЙ МОДУЛЬ
# ─────────────────────────────────────────────

def find_broken_levels(k1, k5, k15, current_price):
    """
    Находит уровни которые были пробиты снизу вверх и держатся.
    Уровень считается пробитым если:
    - Он был сопротивлением (пивот-хай)
    - Цена закрылась выше него минимум на 1 закрытой 5m свече
    - Текущая цена выше него
    Возвращает список уровней отсортированных по цене.
    """
    broken = []
    closes_5m = [float(c[4]) for c in k5]
    times_5m  = [int(c[0]) for c in k5]

    # Собираем пивот-хаи со всех таймфреймов
    pivot_candidates = []
    for candles, tf in [(k1, "1m"), (k5, "5m"), (k15, "15m")]:
        highs = [float(c[2]) for c in candles]
        wing  = 2
        for i in range(wing, len(candles) - wing):
            if highs[i] >= max(highs[i-wing:i] + highs[i+1:i+wing+1]):
                pivot_candidates.append({
                    "price": highs[i],
                    "tf":    tf,
                    "time":  int(candles[i][0])
                })

    # Дедупликация пивотов (убираем слишком близкие)
    pivot_candidates.sort(key=lambda x: x["price"])
    deduped = []
    for pv in pivot_candidates:
        if not deduped or abs(pv["price"] - deduped[-1]["price"]) / pv["price"] * 100 > 0.2:
            deduped.append(pv)

    # Проверяем каждый пивот — был ли он пробит и держится ли
    for pv in deduped:
        price = pv["price"]
        if price >= current_price:
            continue  # выше текущей цены — не интересует

        # Ищем момент пробоя: первая 5m свеча которая закрылась выше уровня
        breakout_idx = None
        for i, (c, t) in enumerate(zip(closes_5m, times_5m)):
            if c > price and t > pv["time"]:
                breakout_idx = i
                break

        if breakout_idx is None:
            continue

        # Проверяем что после пробоя хотя бы INPLAY_HOLD_CANDLES свечей закрылись выше
        held = sum(1 for c in closes_5m[breakout_idx:] if c > price)
        if held < INPLAY_HOLD_CANDLES:
            continue

        # Время удержания в минутах
        hold_mins = (times_5m[-1] - times_5m[breakout_idx]) // 60000

        broken.append({
            "price":      price,
            "tf":         pv["tf"],
            "hold_mins":  hold_mins,
            "breakout_i": breakout_idx,
        })

    broken.sort(key=lambda x: x["price"])
    return broken


def find_round_numbers_in_zone(low, high):
    """Находит круглые числа внутри зоны."""
    rounds = []
    # Определяем шаг круглых чисел по масштабу цены
    mid = (low + high) / 2
    if mid < 0.001:    step = 0.0001
    elif mid < 0.01:   step = 0.001
    elif mid < 0.1:    step = 0.01
    elif mid < 1:      step = 0.1
    elif mid < 10:     step = 0.5
    elif mid < 100:    step = 1.0
    else:              step = 5.0

    n = low // step
    while True:
        r = round((n + 1) * step, 10)
        if r > high:
            break
        if r > low:
            rounds.append(round(r, 10))
        n += 1
    return rounds


def find_inner_levels(k1, k5, zone_low, zone_high):
    """Находит слабые уровни внутри зоны."""
    inner = []
    for candles, tf in [(k1, "1m"), (k5, "5m")]:
        highs = [float(c[2]) for c in candles]
        lows  = [float(c[3]) for c in candles]
        wing  = 2
        for i in range(wing, len(candles) - wing):
            h = highs[i]
            l = lows[i]
            if zone_low < h < zone_high:
                if h >= max(highs[i-wing:i] + highs[i+1:i+wing+1]):
                    inner.append({"price": h, "tf": tf, "type": "res"})
            if zone_low < l < zone_high:
                if l <= min(lows[i-wing:i] + lows[i+1:i+wing+1]):
                    inner.append({"price": l, "tf": tf, "type": "sup"})

    # Дедупликация
    inner.sort(key=lambda x: x["price"])
    deduped = []
    for lv in inner:
        if not deduped or abs(lv["price"] - deduped[-1]["price"]) / lv["price"] * 100 > 0.3:
            deduped.append(lv)
    return deduped


def get_open_interest(symbol):
    """Получает историю OI за последние 30 минут (5m свечи)."""
    data = fetch("https://fapi.binance.com/futures/data/openInterestHist",
                 {"symbol": symbol, "period": "5m", "limit": 10})
    if not data or len(data) < 3:
        return None
    return data


def calc_oi_signal(oi_data, pump_pct):
    """
    Анализирует OI относительно пампа.
    Возвращает (score_delta, oi_str, oi_details).
    """
    if not oi_data or len(oi_data) < 3:
        return 0, "⚪ нет данных", {}

    oi_values = [float(d["sumOpenInterest"]) for d in oi_data]
    oi_start  = oi_values[0]
    oi_now    = oi_values[-1]
    oi_change = (oi_now - oi_start) / oi_start * 100 if oi_start > 0 else 0

    # OI у зоны — последние 3 свечи
    oi_zone_trend = oi_values[-3:]
    oi_holding = oi_zone_trend[-1] >= oi_zone_trend[-2] * 0.98  # не падает
    oi_falling  = oi_zone_trend[-1] < oi_zone_trend[-2] * 0.95  # активно падает

    score = 0
    flags = []

    if oi_change < 10:
        score -= 3
        flags.append(f"⚠️ OI не рос при пампе ({oi_change:+.1f}%) — шортокрыл")
    elif oi_change > 100:
        score -= 2
        flags.append(f"⚠️ OI перегрет ({oi_change:+.1f}%) — лонги под риском")
    elif 10 <= oi_change <= 60:
        score += 2
        flags.append(f"✅ OI органичный ({oi_change:+.1f}%)")
    else:  # 60-100
        score += 1
        flags.append(f"🟡 OI повышен ({oi_change:+.1f}%)")

    if oi_holding:
        score += 2
        flags.append("✅ OI держится у зоны (лонги не сдались)")
    elif oi_falling:
        score -= 2
        flags.append("⚠️ OI падает у зоны (ликвидации идут)")

    oi_str = f"{oi_change:+.1f}% · " + flags[0].split("—")[0].strip() if flags else ""
    return score, oi_str, {
        "change":   round(oi_change, 1),
        "holding":  oi_holding,
        "falling":  oi_falling,
        "flags":    flags,
        "score":    score,
    }


def detect_peak_consolidation(k1, natr):
    """
    Определяет есть ли проторговка на пике.
    Ищет последние N свечей 1m у хая с маленьким телом.
    Возвращает (есть_проторговка, минуты, score_delta, описание).
    """
    if len(k1) < 10:
        return False, 0, 0, "недостаточно данных"

    closes = [float(c[4]) for c in k1]
    highs  = [float(c[2]) for c in k1]
    bodies = [abs(float(c[4]) - float(c[1])) / float(c[2]) * 100
              for c in k1 if float(c[2]) > 0]

    # Находим пик (хай последних 30 свечей)
    peak_idx = highs[-30:].index(max(highs[-30:])) + (len(highs) - 30)
    candles_since_peak = len(k1) - 1 - peak_idx

    if candles_since_peak < 1:
        return False, 0, -3, "только что пик — проторговки нет"

    # Свечи на пике (±2 свечи от хая)
    peak_window = k1[max(0, peak_idx-1): peak_idx+3]
    if not peak_window:
        return False, 0, -3, "нет свечей у пика"

    # Средний размер тела на пике
    peak_bodies = [abs(float(c[4]) - float(c[1])) / ((float(c[2]) + float(c[3])) / 2) * 100
                   for c in peak_window]
    avg_peak_body = sum(peak_bodies) / len(peak_bodies) if peak_bodies else natr

    # Диапазон хай-лоу в окне пика
    peak_highs = [float(c[2]) for c in peak_window]
    peak_lows  = [float(c[3]) for c in peak_window]
    peak_range = (max(peak_highs) - min(peak_lows)) / max(peak_highs) * 100 if peak_highs else natr

    # Нарастание импульса (свечи становятся крупнее)
    last5_bodies = bodies[-5:] if len(bodies) >= 5 else bodies
    prev5_bodies = bodies[-10:-5] if len(bodies) >= 10 else bodies[:5]
    avg_last = sum(last5_bodies) / len(last5_bodies) if last5_bodies else 0
    avg_prev = sum(prev5_bodies) / len(prev5_bodies) if prev5_bodies else 0
    impulse_growing = avg_last > avg_prev * 1.3

    # Проторговка = диапазон пика < 0.5 * NATR и хотя бы 2 мин
    flat_at_peak = peak_range < natr * 0.5 and candles_since_peak >= 2
    consol_mins  = candles_since_peak if flat_at_peak else 0

    if flat_at_peak and consol_mins >= 5:
        score = +3
        desc  = f"✅ Проторговка {consol_mins}мин на пике (диапазон {peak_range:.2f}%)"
    elif flat_at_peak and consol_mins >= 2:
        score = +1
        desc  = f"🟡 Короткая проторговка {consol_mins}мин (диапазон {peak_range:.2f}%)"
    elif impulse_growing:
        score = -3
        desc  = f"🔴 Нарастающий импульс без проторговки — пробой вероятен"
    else:
        score = -2
        desc  = f"⚠️ Нет проторговки на пике — риск вертикального слива"

    return flat_at_peak, consol_mins, score, desc


def calc_zone_quality(k1, k5, natr, zone_high, zone_low,
                      broken_levels, oi_score, peak_score):
    """
    Zone Quality Score 0–10.
    Агрегирует все факторы надёжности зоны.
    """
    zone_pct = (zone_high - zone_low) / zone_high * 100
    score    = 5.0  # базовый
    flags    = []

    # NATR vs зона
    ratio = natr / zone_pct if zone_pct > 0 else 99
    if ratio < 0.5:
        score += 3; flags.append("✅ NATR << зона (зона широкая)")
    elif ratio < 1.0:
        score += 1; flags.append("✅ NATR < зона")
    elif ratio < 1.5:
        score -= 1; flags.append("🟡 NATR ≈ зона")
    else:
        score -= 3; flags.append(f"🔴 NATR ({natr:.1f}%) в {ratio:.1f}x больше зоны — одна свеча перекрывает зону")

    # Верхний уровень (hold_mins)
    top_hold = broken_levels[-1]["hold_mins"] if broken_levels else 0
    if top_hold >= 15:
        score += 2; flags.append(f"✅ Верхний уровень держится {top_hold}мин")
    elif top_hold >= 5:
        score += 1; flags.append(f"🟡 Верхний уровень держится {top_hold}мин")
    else:
        score -= 2; flags.append(f"🔴 Верхний уровень держится {top_hold}мин (не тестирован)")

    # Нижний уровень
    bot_hold = broken_levels[-2]["hold_mins"] if len(broken_levels) >= 2 else 0
    if bot_hold >= 15:
        score += 2; flags.append(f"✅ Нижний уровень держится {bot_hold}мин")
    elif bot_hold >= 5:
        score += 1

    # Паузы в пампе (консолидации)
    closes_1m = [float(c[4]) for c in k1[-30:]]
    natr_slices = []
    for i in range(0, len(closes_1m)-4, 3):
        sl = closes_1m[i:i+4]
        rng = (max(sl)-min(sl))/min(sl)*100 if min(sl) > 0 else 0
        natr_slices.append(rng)
    pauses = sum(1 for s in natr_slices if s < natr * 0.5)
    if pauses >= 2:
        score += 2; flags.append(f"✅ Пауз в пампе: {pauses} (структура)")
    elif pauses == 0:
        score -= 2; flags.append("🔴 Нет пауз в пампе — вертикальный памп")

    # OI и пик
    score += oi_score
    score += peak_score

    score = max(0, min(10, round(score)))

    if score >= 8:
        verdict = "🟢 НАДЁЖНАЯ · полный объём"
        rec_pct = 100
    elif score >= 6:
        verdict = "🟡 УМЕРЕННАЯ · 50% объёма, стоп обязателен"
        rec_pct = 50
    elif score >= 4:
        verdict = "🟠 СЛАБАЯ · 25% объёма, только резерв"
        rec_pct = 25
    else:
        verdict = "🔴 ОПАСНО · не заходить"
        rec_pct = 0

    return score, verdict, rec_pct, flags


def calc_squeeze_probability(k1, k5, zone_high, natr):
    """
    Оценивает вероятность сквиза к зоне.
    Возвращает (вероятность строкой, детали dict).
    """
    closes_1m = [float(c[4]) for c in k1[-20:]]
    vols_1m   = [float(c[7]) for c in k1[-20:]]

    # NATR сжимается?
    natr_early   = calc_natr(k1[-30:-10]) if len(k1) >= 30 else natr
    natr_now     = calc_natr(k1[-10:])   if len(k1) >= 10 else natr
    natr_squeeze = natr_early > 0 and (natr_now / natr_early) < 0.75

    # Объём снижается (накопление)?
    vol_early    = sum(vols_1m[:10]) / 10 if len(vols_1m) >= 10 else 0
    vol_now      = sum(vols_1m[-5:]) / 5  if len(vols_1m) >= 5  else 0
    vol_declining = vol_now < vol_early * 0.7

    # CVD держится положительным
    cvd = calc_cvd(k1)

    # Цена консолидирует у хая
    recent_closes = closes_1m[-5:]
    rng = (max(recent_closes) - min(recent_closes)) / min(recent_closes) * 100 if recent_closes else 0
    consolidating = rng < natr

    score = 0
    if natr_squeeze:   score += 2
    if vol_declining:  score += 1
    if cvd == "up":    score += 1
    if consolidating:  score += 2

    if score >= 5:   prob = "🔴 очень высокая"
    elif score >= 3: prob = "🟡 высокая"
    elif score >= 2: prob = "🟠 умеренная"
    else:            prob = "⚪ низкая"

    return prob, {
        "natr_early":    round(natr_early, 2),
        "natr_now":      round(natr_now, 2),
        "natr_squeeze":  natr_squeeze,
        "vol_declining": vol_declining,
        "cvd":           cvd,
        "consolidating": consolidating,
        "score":         score,
    }


def calc_squeeze_depth(zone_low, zone_high, inner_levels, round_nums, natr):
    """
    Рассчитывает ожидаемую глубину сквиза и расставляет лимитки.
    Возвращает список лимиток с обоснованием.
    """
    zone_width = zone_high - zone_low
    zone_pct   = zone_width / zone_high * 100

    # Все препятствия внутри зоны (уровни + круглые числа), отсортированные сверху вниз
    obstacles = []
    for lv in inner_levels:
        obstacles.append({"price": lv["price"], "reason": f"уровень [{lv['tf']}]", "strength": 1})
    for r in round_nums:
        obstacles.append({"price": r, "reason": "круглое число", "strength": 2})
    obstacles.sort(key=lambda x: x["price"], reverse=True)

    limits = []

    if obstacles:
        # Есть препятствия — концентрируем в верхней половине + резерв внизу
        upper_mid = zone_low + zone_width * 0.6  # верхние 40%

        # Первое препятствие сверху
        first_obstacle = next((o for o in obstacles if o["price"] < zone_high), None)

        if first_obstacle:
            # L1: чуть выше первого препятствия
            l1 = round(first_obstacle["price"] * 1.003, 8)
            if l1 < zone_high:
                limits.append({
                    "price":  l1,
                    "label":  "L1 (агрессивный)",
                    "reason": f"над {first_obstacle['reason']} {first_obstacle['price']:.6g}"
                })
            # L2: у первого препятствия
            limits.append({
                "price":  round(first_obstacle["price"] * 0.999, 8),
                "label":  "L2 (основной)",
                "reason": f"у {first_obstacle['reason']} {first_obstacle['price']:.6g}"
            })

        # L3: резерв у нижней границы
        limits.append({
            "price":  round(zone_low * 1.005, 8),
            "label":  "L3 (резерв)",
            "reason": f"у нижней границы зоны {zone_low:.6g}"
        })
    else:
        # Чистая зона — равномерно на 3 части
        limits = [
            {"price": round(zone_low + zone_width * 0.75, 8),
             "label": "L1 (верх)",   "reason": "верхняя треть зоны"},
            {"price": round(zone_low + zone_width * 0.50, 8),
             "label": "L2 (середина)", "reason": "середина зоны"},
            {"price": round(zone_low + zone_width * 0.20, 8),
             "label": "L3 (низ)",    "reason": "нижняя треть зоны"},
        ]

    # Фильтруем лимитки которые вышли за границы зоны
    limits = [l for l in limits if zone_low <= l["price"] <= zone_high]

    return limits


def build_inplay_chart(symbol, k1, zone_low, zone_high, inner_levels,
                       round_nums, limits, broken_levels):
    """График с зоной, уровнями и лимитками."""
    try:
        data   = k1[-80:]
        n      = len(data)
        opens  = [float(c[1]) for c in data]
        highs  = [float(c[2]) for c in data]
        lows   = [float(c[3]) for c in data]
        closes = [float(c[4]) for c in data]
        dvols  = [float(c[7]) for c in data]
        times  = [datetime.utcfromtimestamp(int(c[0])/1000) for c in data]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9),
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

        # VWAP
        vwap_vals = calc_vwap(data)
        ax1.plot(range(n), vwap_vals, color="#ff9800", lw=1.2,
                 ls="--", alpha=0.85, zorder=6)

        # Зона входа — закрашенная область
        ax1.axhspan(zone_low, zone_high, alpha=0.12, color="#ffd700", zorder=2)
        ax1.axhline(y=zone_high, color="#ffd700", lw=2.0, ls="-",  zorder=6,
                    label=f"Зона верх {zone_high:.6g}")
        ax1.axhline(y=zone_low,  color="#ffd700", lw=2.0, ls="--", zorder=6,
                    label=f"Зона низ {zone_low:.6g}")

        # Пробитые уровни (стек)
        for lv in broken_levels:
            if lv["price"] != zone_high and lv["price"] != zone_low:
                ax1.axhline(y=lv["price"], color="#aaaaaa", lw=0.8, ls=":",
                            alpha=0.6, zorder=4)
                ax1.text(n-0.5, lv["price"],
                         f" {lv['price']:.6g} [{lv['tf']}] {lv['hold_mins']}мин",
                         color="#aaaaaa", fontsize=6, va="center")

        # Внутренние уровни в зоне
        for lv in inner_levels:
            ax1.axhline(y=lv["price"], color="#ff8c00", lw=1.0, ls="--",
                        alpha=0.8, zorder=5)
            ax1.text(1, lv["price"],
                     f" {lv['price']:.6g} [{lv['tf']}] слабый",
                     color="#ff8c00", fontsize=6, va="center")

        # Круглые числа в зоне
        for r in round_nums:
            ax1.axhline(y=r, color="#cc44ff", lw=0.8, ls=":",
                        alpha=0.7, zorder=5)
            ax1.text(3, r, f" {r:.6g} ○",
                     color="#cc44ff", fontsize=6, va="center")

        # Лимитки
        limit_colors = ["#00ff88", "#00cc66", "#009944"]
        for i, lm in enumerate(limits):
            color = limit_colors[min(i, 2)]
            ax1.axhline(y=lm["price"], color=color, lw=1.5, ls="-.",
                        alpha=0.9, zorder=7)
            ax1.text(n * 0.3, lm["price"],
                     f" {lm['label']}: {lm['price']:.6g}",
                     color=color, fontsize=7, va="bottom", fontweight="bold")

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

        ax1.legend(loc="upper left", fontsize=7, facecolor="#161b22",
                   edgecolor="#30363d", labelcolor="white", framealpha=0.8)

        # Volume Profile
        draw_volume_profile(ax1, data)

        ax1.set_title(f"{symbol} · ИНПЛЕЙ · 1m", color="#e6edf3",
                      fontsize=13, fontweight="bold", pad=8)
        ax1.set_ylabel("Price", color="#8b949e", fontsize=9)
        ax2.set_ylabel("Vol USD", color="#8b949e", fontsize=9)
        plt.tight_layout(pad=1.2)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, facecolor="#0d1117")
        plt.close(fig); buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  [INPLAY CHART ERR {symbol}] {e}")
        return None


def build_inplay_alert(symbol, close, pump_pct, pump_mins,
                       zone_low, zone_high, broken_levels,
                       inner_levels, round_nums, limits,
                       squeeze_prob, sq_details, natr, vol_mult_val,
                       oi_str, oi_details, peak_desc,
                       zq_score, zq_verdict, rec_pct, zq_flags,
                       alert_type="inplay"):
    """Строит сообщение инплей-алерта."""
    coin      = symbol.replace("USDT", "")
    zone_pct  = round((zone_high - zone_low) / zone_high * 100, 2)
    dist_high = round((close - zone_high) / zone_high * 100, 2)
    dist_low  = round((close - zone_low)  / zone_low  * 100, 2)

    if alert_type == "inplay":
        header = f"🚀 <code>{symbol}</code> · инплей +{pump_pct:.0f}% за {pump_mins}мин"
    elif alert_type == "approach":
        header = f"⚡️ <code>{symbol}</code> · цена идёт к зоне · −{abs(dist_high):.1f}% до входа"
    elif alert_type == "zone_update":
        header = f"🔄 <code>{symbol}</code> · зона обновлена (новый пробой)"
    elif alert_type == "zone_broken":
        header = f"🚨 <code>{symbol}</code> · ЗОНА ПРОБИТА · убирай лимитки!"
    else:
        header = f"📊 <code>{symbol}</code>"

    lines = [
        header,
        "━━━━━━━━━━━━━━━━━━━",
        f"Цена: <b>{close:.6g}</b>  NATR: {natr:.2f}%  Объём: {vol_mult_val:.1f}x",
        "",
        f"📦 Зона: <b>{zone_low:.6g} – {zone_high:.6g}</b> ({zone_pct}%)",
        f"  ▲ <b>{zone_high:.6g}</b> [{broken_levels[-1]['tf'] if broken_levels else '?'}]"
        f" · до цены: {dist_high:+.2f}%"
        f" · держится {broken_levels[-1]['hold_mins'] if broken_levels else '?'}мин",
        f"  ▼ <b>{zone_low:.6g}</b> [{broken_levels[-2]['tf'] if len(broken_levels)>=2 else '?'}]"
        f" · до цены: {dist_low:+.2f}%",
    ]

    if inner_levels:
        lines.append("  📍 Внутри: " + ", ".join(
            f"{lv['price']:.6g}[{lv['tf']}]" for lv in inner_levels))
    if round_nums:
        lines.append("  🔵 Круглые: " + " / ".join(f"{r:.6g}" for r in round_nums))

    # ── ZONE QUALITY SCORE ──
    lines.append("")
    lines.append(f"🛡 Надёжность зоны: <b>{zq_score}/10</b> — {zq_verdict}")
    for flag in zq_flags[:4]:  # топ-4 фактора
        lines.append(f"  {flag}")

    # OI
    if oi_str:
        lines.append(f"📊 OI: {oi_str}")
        if oi_details.get("flags"):
            for f in oi_details["flags"][1:2]:  # второй флаг если есть
                lines.append(f"  {f}")

    # Пик
    lines.append(f"🏔 Пик: {peak_desc}")

    # Рекомендация по объёму
    lines.append("")
    if rec_pct == 0:
        lines.append("📌 <b>Рекомендация: НЕ ЗАХОДИТЬ</b>")
    else:
        lines.append(f"📌 <b>Рекомендация: {rec_pct}% от объёма</b>")
        lines.append("Лимитки (лонг):")
        for lm in limits:
            lines.append(f"  <b>{lm['label']}: {lm['price']:.6g}</b>  ← {lm['reason']}")

    # Сквиз
    lines.append("")
    lines.append(f"🎯 Сквиз: {squeeze_prob}")
    if sq_details["natr_squeeze"]:
        lines.append(f"  NATR сжимается: {sq_details['natr_early']}% → {sq_details['natr_now']}%")
    if sq_details["vol_declining"]:
        lines.append("  Объём падает (накопление) ✅")
    if sq_details["consolidating"]:
        lines.append("  Консолидирует у хая ✅")

    if alert_type == "zone_broken":
        lines.append("")
        lines.append("⚠️ Нижняя граница пробита вниз на объёме")
        lines.append("Убирай лимитки · следующая поддержка ниже")

    lines.append(f"\n💎 <code>{coin}</code>")
    return "\n".join(lines)


def inplay_scan_symbol(symbol):
    """Проверяет символ на инплей-условия. Возвращает данные или None."""
    k1  = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                {"symbol": symbol, "interval": "1m",  "limit": 120})
    k5  = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                {"symbol": symbol, "interval": "5m",  "limit": 60})
    k15 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                {"symbol": symbol, "interval": "15m", "limit": 30})

    if not k1 or not k5 or len(k1) < 30 or len(k5) < 14:
        return None

    close = float(k1[-1][4])
    natr  = calc_natr(k1)

    if natr < INPLAY_NATR_MIN:
        return None

    # Памп от лоя за последние INPLAY_PUMP_MINS минут
    window     = k1[-INPLAY_PUMP_MINS:]
    lows       = [float(c[3]) for c in window]
    recent_low = min(lows)
    pump_pct   = (close - recent_low) / recent_low * 100
    pump_mins  = INPLAY_PUMP_MINS

    if pump_pct < INPLAY_PUMP_PCT:
        return None

    _, vol_mult_val = volume_mult(k5, lookback=12)
    if vol_mult_val < INPLAY_VOL_MULT:
        return None

    if float(k5[-2][7]) < MIN_VOL_5M_USD:
        return None

    broken = find_broken_levels(k1, k5, k15 or [], close)
    if len(broken) < 2:
        return None

    # Верхний уровень должен держаться минимум 10 мин
    if broken[-1]["hold_mins"] < 10:
        return None

    # Фильтр "памп завершён" — скорость роста последних 5 мин < 50% от пиковой
    closes_1m  = [float(c[4]) for c in k1]
    speeds     = [abs(closes_1m[i] - closes_1m[i-5]) / closes_1m[i-5] * 100
                  for i in range(5, len(closes_1m)) if closes_1m[i-5] > 0]
    if len(speeds) >= 6:
        peak_speed = max(speeds[:-3]) if len(speeds) > 3 else max(speeds)
        curr_speed = sum(speeds[-3:]) / 3
        if peak_speed > 0 and curr_speed > peak_speed * 0.5:
            return None  # памп ещё идёт, зона преждевременна

    zone_high = broken[-1]["price"]
    zone_low  = broken[-2]["price"]
    inner     = find_inner_levels(k1, k5, zone_low, zone_high)
    rounds    = find_round_numbers_in_zone(zone_low, zone_high)

    # OI анализ
    oi_data              = get_open_interest(symbol)
    oi_score, oi_str, oi_details = calc_oi_signal(oi_data, pump_pct)

    # Детектор проторговки на пике
    peak_flat, peak_mins, peak_score, peak_desc = detect_peak_consolidation(k1, natr)

    # Zone Quality Score
    zq_score, zq_verdict, rec_pct, zq_flags = calc_zone_quality(
        k1, k5, natr, zone_high, zone_low,
        broken, oi_score, peak_score
    )

    sq_prob, sq_details = calc_squeeze_probability(k1, k5, zone_high, natr)
    limits = calc_squeeze_depth(zone_low, zone_high, inner, rounds, natr)

    return {
        "close":      close,
        "natr":       natr,
        "pump_pct":   round(pump_pct, 1),
        "pump_mins":  pump_mins,
        "vol_mult":   vol_mult_val,
        "broken":     broken,
        "zone_high":  zone_high,
        "zone_low":   zone_low,
        "inner":      inner,
        "rounds":     rounds,
        "sq_prob":    sq_prob,
        "sq_details": sq_details,
        "limits":     limits,
        "oi_str":     oi_str,
        "oi_details": oi_details,
        "peak_flat":  peak_flat,
        "peak_mins":  peak_mins,
        "peak_desc":  peak_desc,
        "zq_score":   zq_score,
        "zq_verdict": zq_verdict,
        "rec_pct":    rec_pct,
        "zq_flags":   zq_flags,
        "k1":         k1,
    }


def can_inplay_alert(symbol, alert_type, cooldown=120):
    now = time.time()
    key = f"{symbol}_{alert_type}"
    if now - inplay_alert_cache.get(key, 0) >= cooldown:
        inplay_alert_cache[key] = now
        return True
    return False


def inplay_loop(symbols_ref):
    """Бесконечный цикл инплей-модуля."""
    print("  [IP] Инплей модуль запущен")
    while True:
        start   = time.time()
        symbols = symbols_ref[0]
        if not symbols:
            time.sleep(INPLAY_SCAN_SEC)
            continue

        def process_inplay(sym):
            try:
                data = inplay_scan_symbol(sym)

                with inplay_lock:
                    prev = inplay_coins.get(sym)

                if data is None:
                    # Проверяем пробой зоны если монета была в наблюдении
                    if prev and not prev.get("zone_broken_alerted"):
                        k1_q = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                                     {"symbol": sym, "interval": "1m", "limit": 10})
                        if k1_q:
                            cur = float(k1_q[-1][4])
                            if cur < prev["zone_low"] * 0.998:  # пробой нижней границы
                                msg = build_inplay_alert(
                                    sym, cur, prev["pump_pct"], prev["pump_mins"],
                                    prev["zone_low"], prev["zone_high"],
                                    prev["broken"], prev["inner"], prev["rounds"],
                                    prev["limits"], prev["sq_prob"], prev["sq_details"],
                                    prev["natr"], prev["vol_mult"],
                                    prev["oi_str"], prev["oi_details"], prev["peak_desc"],
                                    prev["zq_score"], prev["zq_verdict"], prev["rec_pct"], prev["zq_flags"],
                                    alert_type="zone_broken"
                                )
                                k1_broken = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                                                  {"symbol": sym, "interval": "1m", "limit": 80})
                                chart_broken = build_inplay_chart(
                                    sym, k1_broken or [],
                                    prev["zone_low"], prev["zone_high"],
                                    prev.get("inner",[]), prev.get("rounds",[]),
                                    prev.get("limits",[]), prev.get("broken",[])
                                ) if k1_broken else None
                                if chart_broken:
                                    send_photo(chart_broken, msg)
                                else:
                                    send_message(msg)
                                print(f"  [IP] 🚨 {sym} зона пробита")
                                with inplay_lock:
                                    if sym in inplay_coins:
                                        inplay_coins[sym]["zone_broken_alerted"] = True
                    return

                close     = data["close"]
                zone_high = data["zone_high"]
                zone_low  = data["zone_low"]

                # Алерт 1: новая инплей монета
                if prev is None:
                    if can_inplay_alert(sym, "inplay", cooldown=300):
                        caption = build_inplay_alert(
                            sym, close, data["pump_pct"], data["pump_mins"],
                            zone_low, zone_high, data["broken"],
                            data["inner"], data["rounds"], data["limits"],
                            data["sq_prob"], data["sq_details"],
                            data["natr"], data["vol_mult"],
                            data["oi_str"], data["oi_details"], data["peak_desc"],
                            data["zq_score"], data["zq_verdict"], data["rec_pct"], data["zq_flags"],
                            alert_type="inplay"
                        )
                        chart = build_inplay_chart(
                            sym, data["k1"], zone_low, zone_high,
                            data["inner"], data["rounds"], data["limits"],
                            data["broken"]
                        )
                        if chart: send_photo(chart, caption)
                        else:     send_message(caption)
                        # Регистрируем сигнал для контроля
                        register_signal(sym, zone_low, zone_high,
                                        data["zq_score"], data["zq_verdict"],
                                        data["rec_pct"], close,
                                        k1=data["k1"], peak_mins=data["peak_mins"])
                        print(f"  [IP] 🚀 {sym} инплей +{data['pump_pct']}%")

                    with inplay_lock:
                        inplay_coins[sym] = {**data, "since": time.time(),
                                             "zone_broken_alerted": False,
                                             "last_approach_alert": 0}

                else:
                    # Алерт 2: зона обновилась (новый пробой)
                    if (abs(zone_high - prev["zone_high"]) / prev["zone_high"] * 100 > 0.3 or
                        abs(zone_low  - prev["zone_low"])  / prev["zone_low"]  * 100 > 0.3):
                        if can_inplay_alert(sym, "zone_update", cooldown=120):
                            caption = build_inplay_alert(
                                sym, close, data["pump_pct"], data["pump_mins"],
                                zone_low, zone_high, data["broken"],
                                data["inner"], data["rounds"], data["limits"],
                                data["sq_prob"], data["sq_details"],
                                data["natr"], data["vol_mult"],
                                data["oi_str"], data["oi_details"], data["peak_desc"],
                                data["zq_score"], data["zq_verdict"], data["rec_pct"], data["zq_flags"],
                                alert_type="zone_update"
                            )
                            chart = build_inplay_chart(
                                sym, data["k1"], zone_low, zone_high,
                                data["inner"], data["rounds"], data["limits"],
                                data["broken"]
                            )
                            if chart: send_photo(chart, caption)
                            else:     send_message(caption)
                            print(f"  [IP] 🔄 {sym} зона обновлена")

                    # Алерт 3: цена идёт к зоне
                    dist_to_zone = (close - zone_high) / zone_high * 100
                    now = time.time()
                    last_approach = prev.get("last_approach_alert", 0)
                    if (0 < dist_to_zone <= ZONE_APPROACH_PCT and
                            now - last_approach > 180):
                        caption = build_inplay_alert(
                            sym, close, data["pump_pct"], data["pump_mins"],
                            zone_low, zone_high, data["broken"],
                            data["inner"], data["rounds"], data["limits"],
                            data["sq_prob"], data["sq_details"],
                            data["natr"], data["vol_mult"],
                            data["oi_str"], data["oi_details"], data["peak_desc"],
                            data["zq_score"], data["zq_verdict"], data["rec_pct"], data["zq_flags"],
                            alert_type="approach"
                        )
                        chart = build_inplay_chart(
                            sym, data["k1"], zone_low, zone_high,
                            data["inner"], data["rounds"], data["limits"],
                            data["broken"]
                        )
                        if chart: send_photo(chart, caption)
                        else:     send_message(caption)
                        print(f"  [IP] ⚡ {sym} идёт к зоне dist={dist_to_zone:.1f}%")

                    with inplay_lock:
                        inplay_coins[sym] = {**data, "since": prev.get("since", time.time()),
                                             "zone_broken_alerted": False,
                                             "last_approach_alert": now if 0 < dist_to_zone <= ZONE_APPROACH_PCT else last_approach}

            except Exception as e:
                print(f"  [IP ERR {sym}] {e}")

        # Убираем протухшие
        now = time.time()
        with inplay_lock:
            expired = [s for s, v in inplay_coins.items()
                       if now - v.get("since", 0) > INPLAY_TTL]
            for s in expired:
                del inplay_coins[s]
                print(f"  [IP] ⏰ {s} убран из инплей")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            list(pool.map(process_inplay, symbols))

        elapsed = time.time() - start
        time.sleep(max(0, INPLAY_SCAN_SEC - elapsed))


# ─────────────────────────────────────────────
#  ДЕТЕКТОР ПСЕВДОПАМПА
# ─────────────────────────────────────────────

def detect_pseudo_pump(symbol, k1, k5):
    """
    Определяет является ли памп псевдопампом (управляемым движением).

    Критерии:
    1. Объём ДО пампа < 5% от объёма пампа (тихий флэт перед взлётом)
    2. Скорость пампа > PSEUDO_PUMP_SPEED % за 15 минут
    3. OI изменился < PSEUDO_OI_CHANGE % (не новые позиции — перекладка)
    4. После хая нет ни одной зелёной свечи с объёмом > 50% от средней

    Возвращает (is_pseudo, score, details_dict).
    """
    if not k1 or not k5 or len(k1) < 30 or len(k5) < 14:
        return False, 0, {}

    closes_1m = [float(c[4]) for c in k1]
    highs_1m  = [float(c[2]) for c in k1]
    vols_1m   = [float(c[7]) for c in k1]
    vols_5m   = [float(c[7]) for c in k5]

    # ── Находим пик и момент начала пампа ──
    peak_idx  = highs_1m.index(max(highs_1m[-60:]))  # пик за последний час
    peak_high = highs_1m[peak_idx]
    close_now = closes_1m[-1]

    # Сколько минут назад был пик
    mins_since_peak = len(k1) - 1 - peak_idx
    if mins_since_peak < 2:
        return False, 0, {}  # пик только что — рано судить

    # ── Критерий 1: Объём до пампа vs объём во время пампа ──
    # Ищем начало пампа — первую свечу с ростом > 1% от предыдущего лоя
    pump_start_idx = peak_idx
    for i in range(peak_idx, max(0, peak_idx - 20), -1):
        if i == 0:
            break
        pct_chg = (float(k1[i][4]) - float(k1[i-1][4])) / float(k1[i-1][4]) * 100
        if abs(pct_chg) < 0.5:
            pump_start_idx = i - 1
        else:
            break

    # Объём ДО пампа (2 часа до старта, берём из 5m)
    pre_pump_candles = max(1, pump_start_idx // 5)  # примерно в 5m-свечах
    pre_pump_vol  = sum(vols_5m[max(0, len(vols_5m) - pre_pump_candles - 24):
                                 max(0, len(vols_5m) - pre_pump_candles)]) / \
                    max(1, min(24, pre_pump_candles))
    pump_vol      = sum(vols_1m[pump_start_idx:peak_idx + 1]) / max(1, peak_idx - pump_start_idx + 1)

    vol_ratio = pre_pump_vol / pump_vol if pump_vol > 0 else 1.0
    crit1_vol = vol_ratio < PSEUDO_VOL_RATIO

    # ── Критерий 2: Скорость пампа ──
    pump_duration_mins = max(1, peak_idx - pump_start_idx)
    pump_pct = (peak_high - float(k1[pump_start_idx][3])) / float(k1[pump_start_idx][3]) * 100
    pump_speed_per_15 = (pump_pct / pump_duration_mins) * 15  # нормализуем к 15мин
    crit2_speed = pump_speed_per_15 > PSEUDO_PUMP_SPEED

    # ── Критерий 3: OI почти не менялся ──
    oi_data   = get_open_interest(symbol)
    oi_change = 0.0
    if oi_data and len(oi_data) >= 3:
        oi_vals   = [float(d["sumOpenInterest"]) for d in oi_data]
        oi_start  = oi_vals[0]
        oi_peak   = max(oi_vals)
        oi_change = abs(oi_peak - oi_start) / oi_start * 100 if oi_start > 0 else 0
    crit3_oi = oi_change < PSEUDO_OI_CHANGE

    # ── Критерий 4: После пика нет зелёных свечей с объёмом ──
    post_peak = k1[peak_idx + 1:]
    avg_vol_post = sum(vols_1m[-20:]) / 20 if len(vols_1m) >= 20 else 1
    green_with_vol = sum(
        1 for c in post_peak
        if float(c[4]) >= float(c[1])                        # зелёная
        and float(c[7]) > avg_vol_post * 0.5                 # объём > 50% среднего
    )
    crit4_no_buyers = green_with_vol == 0 and len(post_peak) >= 3

    # ── Итоговый скор (0–4) ──
    score = sum([crit1_vol, crit2_speed, crit3_oi, crit4_no_buyers])

    # Псевдопамп если выполнены минимум 3 из 4 критериев
    is_pseudo = score >= 3

    # ── Уровень флэта ДО пампа (pre-pump support) ──
    pre_pump_window = k1[max(0, pump_start_idx - 30): pump_start_idx + 1]
    if pre_pump_window:
        pre_lows  = [float(c[3]) for c in pre_pump_window]
        pre_highs = [float(c[2]) for c in pre_pump_window]
        pre_pump_low  = sum(sorted(pre_lows)[:max(1, len(pre_lows)//3)]) / max(1, len(pre_lows)//3)
        pre_pump_high = sum(sorted(pre_highs)[-max(1, len(pre_highs)//3):]) / max(1, len(pre_highs)//3)
    else:
        pre_pump_low  = float(k1[pump_start_idx][3])
        pre_pump_high = float(k1[pump_start_idx][4])

    # ── Зона ретеста хая ──
    atr = calc_atr(k1)
    retest_high = peak_high + atr * 0.5
    retest_low  = peak_high - atr * 0.5

    details = {
        "is_pseudo":       is_pseudo,
        "score":           score,
        "pump_pct":        round(pump_pct, 1),
        "pump_mins":       pump_duration_mins,
        "pump_speed_15":   round(pump_speed_per_15, 1),
        "vol_ratio":       round(vol_ratio * 100, 1),   # в %
        "oi_change":       round(oi_change, 1),
        "green_with_vol":  green_with_vol,
        "crit_vol":        crit1_vol,
        "crit_speed":      crit2_speed,
        "crit_oi":         crit3_oi,
        "crit_no_buyers":  crit4_no_buyers,
        "peak_high":       peak_high,
        "pre_pump_low":    round(pre_pump_low, 8),
        "pre_pump_high":   round(pre_pump_high, 8),
        "retest_high":     round(retest_high, 8),
        "retest_low":      round(retest_low, 8),
        "close_now":       close_now,
    }
    return is_pseudo, score, details


def build_pseudo_alert(symbol, d):
    """Строит Telegram-сообщение для псевдопампа."""
    coin = symbol.replace("USDT", "")
    p    = d["peak_high"]
    natr_stop_buf = abs(d["retest_high"] - p) * 0.3  # буфер для стопа

    # Стоп для шорта на ретесте — чуть выше хая
    short_stop = round(p * 1.005, 8)

    # Стоп для лонга у pre-pump — чуть ниже уровня
    long_stop  = round(d["pre_pump_low"] * 0.993, 8)

    # Цель шорта — pre-pump уровень
    short_target = round((d["pre_pump_low"] + d["pre_pump_high"]) / 2, 8)

    # Цель лонга — +4%
    long_target = round(d["pre_pump_high"] * 1.04, 8)

    # Признаки
    crits = []
    if d["crit_vol"]:
        crits.append(f"📉 Объём до пампа: {d['vol_ratio']:.1f}% от пампового (тихий флэт)")
    if d["crit_speed"]:
        crits.append(f"⚡ Скорость: +{d['pump_pct']:.0f}% за {d['pump_mins']}мин"
                     f" ({d['pump_speed_15']:.0f}%/15мин)")
    if d["crit_oi"]:
        crits.append(f"🔄 OI изменился только на {d['oi_change']:.1f}% — не новые позиции")
    if d["crit_no_buyers"]:
        crits.append("🚫 После хая — ноль покупателей, только красные свечи")

    lines = [
        f"⚠️ ПСЕВДОПАМП · <code>{symbol}</code>",
        "━━━━━━━━━━━━━━━━━━━",
        f"Памп +{d['pump_pct']:.0f}% за {d['pump_mins']}мин — признаки управляемого движения",
        f"Совпавших критериев: <b>{d['score']}/4</b>",
        "",
        "🔍 Признаки:",
    ] + crits + [
        "",
        f"📍 Pre-pump уровень (откуда начинали): <b>{d['pre_pump_low']:.6g} – {d['pre_pump_high']:.6g}</b>",
        f"📍 Хай пампа: <b>{d['peak_high']:.6g}</b>",
        "",
        f"📉 <b>Шорт-зона (ретест хая):</b> {d['retest_low']:.6g} – {d['retest_high']:.6g}",
        f"   Цель: {short_target:.6g}  ·  Стоп: выше {short_stop:.6g}",
        f"   R/R ≈ 1:{round(abs(d['peak_high'] - short_target) / abs(short_stop - d['peak_high']), 0):.0f}",
        "",
        f"📈 <b>Лонг-зона (у старта):</b> {d['pre_pump_low']:.6g} – {d['pre_pump_high']:.6g}",
        f"   Цель: {long_target:.6g}  ·  Стоп: ниже {long_stop:.6g}",
        "",
        f"💎 <code>{coin}</code>",
    ]
    return "\n".join(lines)


def build_pseudo_chart(symbol, k1, d):
    """График псевдопампа: свечи + зоны шорта и лонга."""
    try:
        data   = k1[-80:]
        n      = len(data)
        opens  = [float(c[1]) for c in data]
        highs  = [float(c[2]) for c in data]
        lows   = [float(c[3]) for c in data]
        closes = [float(c[4]) for c in data]
        dvols  = [float(c[7]) for c in data]
        times  = [datetime.utcfromtimestamp(int(c[0])/1000) for c in data]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 9),
                                        gridspec_kw={"height_ratios": [3, 1]},
                                        facecolor="#0d1117")
        ax1.set_facecolor("#0d1117")
        ax2.set_facecolor("#0d1117")
        w = 0.6

        for i in range(n):
            color = "#26a69a" if closes[i] >= opens[i] else "#ef5350"
            ax1.plot([i, i], [lows[i], highs[i]], color=color, lw=0.8)
            bh = abs(closes[i] - opens[i]) or (highs[i] - lows[i]) * 0.01
            ax1.add_patch(Rectangle((i - w/2, min(opens[i], closes[i])), w, bh,
                                     facecolor=color, edgecolor=color))
            ax2.bar(i, dvols[i], color=color, width=w, alpha=0.85)

        # Шорт-зона (красная) — ретест хая
        ax1.axhspan(d["retest_low"], d["retest_high"], alpha=0.15, color="#ef5350", zorder=2)
        ax1.axhline(y=d["retest_high"], color="#ef5350", lw=1.8, ls="-",
                    label=f"Шорт-зона {d['retest_low']:.6g}–{d['retest_high']:.6g}", zorder=6)
        ax1.axhline(y=d["retest_low"],  color="#ef5350", lw=1.2, ls="--", zorder=6)

        # Хай пампа
        ax1.axhline(y=d["peak_high"], color="#ff8800", lw=1.5, ls=":",
                    label=f"Хай пампа {d['peak_high']:.6g}", zorder=6)

        # Лонг-зона (зелёная) — pre-pump
        ax1.axhspan(d["pre_pump_low"], d["pre_pump_high"], alpha=0.15, color="#26a69a", zorder=2)
        ax1.axhline(y=d["pre_pump_high"], color="#26a69a", lw=1.8, ls="-",
                    label=f"Лонг-зона {d['pre_pump_low']:.6g}–{d['pre_pump_high']:.6g}", zorder=6)
        ax1.axhline(y=d["pre_pump_low"],  color="#26a69a", lw=1.2, ls="--", zorder=6)

        # Заголовок с признаками
        crit_str = f"{d['score']}/4 крит · +{d['pump_pct']:.0f}% за {d['pump_mins']}мин"
        ax1.set_title(f"⚠️ {symbol} · ПСЕВДОПАМП · {crit_str}",
                      color="#ff8800", fontsize=12, fontweight="bold", pad=8)

        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_usd(x)))
        ticks = list(range(0, n, max(1, n // 6)))
        for ax in [ax1, ax2]:
            ax.set_xticks(ticks)
            ax.set_xticklabels([times[i].strftime("%H:%M") for i in ticks],
                                color="#8b949e", fontsize=8)
            ax.tick_params(colors="#8b949e", labelsize=8)
            ax.yaxis.set_tick_params(labelcolor="#8b949e")
            for sp in ax.spines.values():
                sp.set_edgecolor("#30363d")
            ax.grid(color="#21262d", ls="--", lw=0.5)
            ax.set_xlim(-1, n)

        ax1.legend(loc="upper left", fontsize=7, facecolor="#161b22",
                   edgecolor="#30363d", labelcolor="white", framealpha=0.8)
        ax1.set_ylabel("Price", color="#8b949e", fontsize=9)
        ax2.set_ylabel("Vol USD", color="#8b949e", fontsize=9)
        plt.tight_layout(pad=1.2)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, facecolor="#0d1117")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  [PSEUDO CHART ERR {symbol}] {e}")
        return None


def can_pseudo_alert(symbol, cooldown=300):
    now = time.time()
    key = f"pseudo_{symbol}"
    if now - pseudo_alert_cache.get(key, 0) >= cooldown:
        pseudo_alert_cache[key] = now
        return True
    return False


def pseudo_scan_symbol(symbol):
    """Проверяет символ на псевдопамп. Возвращает (is_pseudo, details) или None."""
    k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "1m", "limit": 120})
    k5 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "5m", "limit": 60})

    if not k1 or not k5 or len(k1) < 30:
        return None

    # Быстрый фильтр: был ли памп вообще?
    closes   = [float(c[4]) for c in k1]
    highs    = [float(c[2]) for c in k1]
    peak_h   = max(highs[-60:])
    trough_l = min(float(c[3]) for c in k1[-60:])
    pump_pct = (peak_h - trough_l) / trough_l * 100 if trough_l > 0 else 0

    if pump_pct < INPLAY_PUMP_PCT:
        return None  # не было пампа — нечего анализировать

    # Объём достаточный?
    vols_5m = [float(c[7]) for c in k5]
    if max(vols_5m[-12:]) < MIN_VOL_SPIKE_ABS:
        return None

    is_pseudo, score, details = detect_pseudo_pump(symbol, k1, k5)
    if not is_pseudo:
        return None

    return details


def pseudo_loop(symbols_ref):
    """Отдельный поток: сканирует все символы на псевдопампы."""
    print("  [PSEUDO] Детектор псевдопампов запущен")
    while True:
        start   = time.time()
        symbols = symbols_ref[0]
        if not symbols:
            time.sleep(PSEUDO_SCAN_SEC)
            continue

        def process_pseudo(sym):
            try:
                result = pseudo_scan_symbol(sym)
                if result is None:
                    # Не псевдопамп — убираем из кэша если был
                    with pseudo_lock:
                        pseudo_coins.pop(sym, None)
                    return

                d = result
                with pseudo_lock:
                    prev = pseudo_coins.get(sym)

                # Новый псевдопамп или хай изменился значительно
                if prev is None or abs(d["peak_high"] - prev.get("peak_high", 0)) / d["peak_high"] * 100 > 1.0:
                    if can_pseudo_alert(sym):
                        caption = build_pseudo_alert(sym, d)
                        k1_raw  = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                                        {"symbol": sym, "interval": "1m", "limit": 120})
                        chart   = build_pseudo_chart(sym, k1_raw, d) if k1_raw else None
                        if chart:
                            send_photo(chart, caption)
                        else:
                            send_message(caption)
                        print(f"  [PSEUDO] ⚠️ {sym} псевдопамп {d['score']}/4 крит"
                              f" +{d['pump_pct']}% за {d['pump_mins']}мин")

                    with pseudo_lock:
                        pseudo_coins[sym] = {**d, "since": time.time()}

                # ── Алерт: цена вернулась к ретест-зоне хая ──
                close_now = d["close_now"]
                if d["retest_low"] <= close_now <= d["retest_high"]:
                    key = f"pseudo_retest_{sym}"
                    now = time.time()
                    if now - pseudo_alert_cache.get(key, 0) >= 180:
                        pseudo_alert_cache[key] = now
                        msg = (
                            f"🎯 <code>{sym}</code> · РЕТЕСТ ХАЯ ПСЕВДОПАМПА\n"
                            f"Цена вернулась к зоне шорта: "
                            f"<b>{d['retest_low']:.6g}–{d['retest_high']:.6g}</b>\n"
                            f"Цель: {d['pre_pump_low']:.6g} · "
                            f"Стоп: {round(d['peak_high'] * 1.005, 8):.6g}\n"
                            f"⚠️ Инициирован псевдопамп — зона для шорта"
                        )
                        send_message(msg)
                        print(f"  [PSEUDO] 🎯 {sym} ретест хая псевдопампа")

                # ── Алерт: цена вернулась к pre-pump зоне ──
                if d["pre_pump_low"] * 0.995 <= close_now <= d["pre_pump_high"] * 1.005:
                    key = f"pseudo_prepump_{sym}"
                    now = time.time()
                    if now - pseudo_alert_cache.get(key, 0) >= 180:
                        pseudo_alert_cache[key] = now
                        msg = (
                            f"📈 <code>{sym}</code> · PRE-PUMP УРОВЕНЬ\n"
                            f"Цена у старта пампа: "
                            f"<b>{d['pre_pump_low']:.6g}–{d['pre_pump_high']:.6g}</b>\n"
                            f"Маркетмейкер закрывает шорты здесь → лонг\n"
                            f"Цель: +4% · Стоп: ниже {round(d['pre_pump_low'] * 0.993, 8):.6g}\n"
                            f"💎 <code>{sym.replace('USDT', '')}</code>"
                        )
                        send_message(msg)
                        print(f"  [PSEUDO] 📈 {sym} pre-pump уровень достигнут")

            except Exception as e:
                print(f"  [PSEUDO ERR {sym}] {e}")

        # Фильтруем только те монеты где был объём-спайк (из active + inplay)
        with active_lock:
            watched = set(active_coins.keys())
        with inplay_lock:
            watched |= set(inplay_coins.keys())
        # + монеты уже в псевдо-кэше (продолжаем мониторить)
        with pseudo_lock:
            watched |= set(pseudo_coins.keys())

        # Лимитируем сканирование по активным монетам
        scan_list = [s for s in symbols if s in watched] or list(watched)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            list(pool.map(process_pseudo, scan_list))

        # Удаляем протухшие
        now = time.time()
        with pseudo_lock:
            expired = [s for s, v in pseudo_coins.items()
                       if now - v.get("since", 0) > PSEUDO_TTL]
            for s in expired:
                del pseudo_coins[s]
                print(f"  [PSEUDO] ⏰ {s} убран из псевдопамп-кэша")

        elapsed = time.time() - start
        time.sleep(max(0, PSEUDO_SCAN_SEC - elapsed))




import json

STATS_FILE = "/tmp/signal_stats.json"

# Структура записи: { id, symbol, time, zone_low, zone_high, zq_score,
#                    zq_verdict, rec_pct, close_at_signal,
#                    result: None | "hit_zone"/"bounced"/"broke_zone"/"no_touch",
#                    result_time, result_close, result_pct }
signal_tracker: dict = {}
tracker_lock = threading.Lock()


def load_stats():
    global signal_tracker
    try:
        with open(STATS_FILE, "r") as f:
            signal_tracker = json.load(f)
    except Exception:
        signal_tracker = {}


def save_stats():
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(signal_tracker, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [STATS] Ошибка сохранения: {e}")


def register_signal(symbol, zone_low, zone_high, zq_score, zq_verdict,
                    rec_pct, close_now, k1=None, peak_mins=0):
    """Регистрирует новый сигнал для последующего контроля."""
    sid = f"{symbol}_{int(time.time())}"

    # Объём проторговки — среднее за последние 10 свечей (органичность)
    vol_during_pump = 0
    vol_at_peak     = 0
    if k1 and len(k1) >= 15:
        vols = [float(c[7]) for c in k1]
        vol_during_pump = sum(vols[-20:-5]) / 15 if len(vols) >= 20 else sum(vols[:-5]) / max(1, len(vols)-5)
        vol_at_peak     = sum(vols[-5:]) / 5

    entry = {
        "id":               sid,
        "symbol":           symbol,
        "time":             datetime.now().strftime("%Y-%m-%d %H:%M"),
        "zone_low":         zone_low,
        "zone_high":        zone_high,
        "zq_score":         zq_score,
        "zq_verdict":       zq_verdict,
        "rec_pct":          rec_pct,
        "close_at_signal":  close_now,
        "peak_mins":        peak_mins,
        "vol_during_pump":  round(vol_during_pump, 0),
        "vol_at_peak":      round(vol_at_peak, 0),
        "zone_touch_time":  None,   # когда цена коснулась зоны
        "result":           None,
        "result_time":      None,
        "result_close":     None,
        "result_detail":    None,
        "travel_mins":      None,   # сколько минут шла от пика до зоны
        "mini_bounces":     [],     # мини отскоки по пути
    }
    with tracker_lock:
        signal_tracker[sid] = entry
    save_stats()
    return sid


def _detect_mini_bounces(k1_range, zone_high, natr):
    """
    Находит мини-отскоки в коррекции от пика до зоны.
    Ищет свечи где цена отскочила от круглого числа или уровня на ≥ 0.3×NATR.
    """
    bounces = []
    if not k1_range or len(k1_range) < 3:
        return bounces

    closes = [float(c[4]) for c in k1_range]
    lows   = [float(c[3]) for c in k1_range]

    for i in range(1, len(k1_range) - 1):
        lo  = lows[i]
        # Локальный минимум
        if lo > lows[i-1] or lo > lows[i+1]:
            continue
        # Отскок от этого лоя
        rebound = (closes[i+1] - lo) / lo * 100 if lo > 0 else 0
        if rebound < natr * 0.3:
            continue

        # Определяем причину отскока
        reason = "уровень"
        # Круглое число?
        rounds = find_round_numbers_in_zone(lo * 0.999, lo * 1.001)
        if rounds:
            reason = f"круглое {rounds[0]:.6g}"

        bounces.append({
            "price":   round(lo, 8),
            "rebound": round(rebound, 2),
            "reason":  reason,
            "time":    datetime.utcfromtimestamp(int(k1_range[i][0])/1000).strftime("%H:%M"),
        })

    return bounces


def check_signal_results():
    """
    Проверяет результаты незакрытых сигналов каждые 2 минуты.
    Отслеживает: время движения от пика до зоны, мини-отскоки, дивергенцию объёма.
    """
    now = time.time()
    with tracker_lock:
        pending = {sid: s for sid, s in signal_tracker.items()
                   if s["result"] is None}

    for sid, sig in pending.items():
        symbol    = sig["symbol"]
        zone_low  = sig["zone_low"]
        zone_high = sig["zone_high"]
        signal_ts = time.mktime(datetime.strptime(sig["time"], "%Y-%m-%d %H:%M").timetuple())

        if now - signal_ts > 14400:
            with tracker_lock:
                signal_tracker[sid]["result"]        = "no_touch"
                signal_tracker[sid]["result_time"]   = datetime.now().strftime("%H:%M")
                signal_tracker[sid]["result_detail"] = "Зона не была достигнута за 4ч"
            save_stats()
            _send_result_alert(signal_tracker[sid])
            continue

        k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                   {"symbol": symbol, "interval": "1m", "limit": 60})
        if not k1:
            continue

        close = float(k1[-1][4])
        pct   = (close - sig["close_at_signal"]) / sig["close_at_signal"] * 100
        natr  = calc_natr(k1)

        # Время движения от пика до зоны
        highs = [float(c[2]) for c in k1]
        peak_idx = len(highs) - 1 - highs[::-1].index(max(highs))
        travel_mins = len(k1) - 1 - peak_idx

        # Мини-отскоки в коррекции
        correction_candles = k1[peak_idx:] if peak_idx < len(k1) else []
        mini_bounces = _detect_mini_bounces(correction_candles, zone_high, natr)

        result = None
        detail = None

        if zone_low <= close <= zone_high:
            result = "hit_zone"
            detail = f"Цена вошла в зону · {close:.6g} ({pct:+.1f}%)"
        elif close < zone_low * 0.998:
            result = "broke_zone"
            detail = f"Зона пробита вниз · {close:.6g} ({pct:+.1f}%)"
        elif close > zone_high * 1.02 and pct > 3:
            result = "bounced"
            detail = f"Отскок от зоны вверх · {close:.6g} ({pct:+.1f}%) · зона держала"

        # Обновляем travel_mins и mini_bounces всегда
        with tracker_lock:
            signal_tracker[sid]["travel_mins"]  = travel_mins
            signal_tracker[sid]["mini_bounces"] = mini_bounces
            if sig.get("zone_touch_time") is None and zone_low <= close <= zone_high:
                signal_tracker[sid]["zone_touch_time"] = datetime.now().strftime("%H:%M")

        if result:
            with tracker_lock:
                signal_tracker[sid]["result"]       = result
                signal_tracker[sid]["result_time"]  = datetime.now().strftime("%H:%M")
                signal_tracker[sid]["result_close"] = close
                signal_tracker[sid]["result_detail"] = detail
                signal_tracker[sid]["k1_snapshot"]  = k1  # для графика
            save_stats()
            _send_result_alert(signal_tracker[sid], k1)


def _send_result_alert(sig, k1=None):
    """Отправляет результат отработки сигнала с графиком и расширенным анализом."""
    result = sig["result"]
    symbol = sig["symbol"]
    coin   = symbol.replace("USDT", "")
    zone_low  = sig["zone_low"]
    zone_high = sig["zone_high"]

    icons = {
        "hit_zone":   "📍",
        "bounced":    "✅",
        "broke_zone": "🚨",
        "no_touch":   "⏰",
    }
    titles = {
        "hit_zone":   "цена в зоне",
        "bounced":    "отскок — зона держала",
        "broke_zone": "зона пробита вниз",
        "no_touch":   "зона не достигнута",
    }

    rec    = sig["rec_pct"]
    assess = ""
    if result == "bounced":
        assess = "✅ Оценка верная" if rec >= 50 else "⚠️ Оценка занижена — стоило заходить"
    elif result == "broke_zone":
        assess = "✅ Оценка верная" if rec == 0 else f"⚠️ Оценка завышена — рекомендовали {rec}%"
    elif result == "no_touch":
        assess = "ℹ️ Зона не тестировалась"
    elif result == "hit_zone":
        assess = "👀 Цена в зоне — ждём результат"

    # Дивергенция объёма проторговки
    vp = sig.get("vol_during_pump", 0)
    va = sig.get("vol_at_peak", 0)
    if vp > 0 and va > 0:
        vol_ratio = va / vp
        if vol_ratio < 0.4:
            vol_div = f"⚠️ Объём на пике упал до {vol_ratio*100:.0f}% от пампа (дивергенция)"
        elif vol_ratio < 0.7:
            vol_div = f"🟡 Объём на пике умеренный ({vol_ratio*100:.0f}% от пампа)"
        else:
            vol_div = f"✅ Объём на пике держится ({vol_ratio*100:.0f}% от пампа)"
    else:
        vol_div = ""

    # Время движения от пика до зоны
    travel = sig.get("travel_mins")
    if travel is not None:
        if travel >= 15:
            travel_str = f"✅ Время коррекции: {travel}мин (медленно — зона надёжнее)"
        elif travel >= 5:
            travel_str = f"🟡 Время коррекции: {travel}мин (умеренно)"
        else:
            travel_str = f"⚠️ Время коррекции: {travel}мин (быстро — риск пробоя)"
    else:
        travel_str = ""

    # Проторговка на пике
    peak_mins = sig.get("peak_mins", 0)
    peak_str  = f"Проторговка на пике: {peak_mins}мин" if peak_mins else "Проторговки на пике не было"

    # Мини отскоки
    bounces = sig.get("mini_bounces", [])
    if bounces:
        b_lines = [f"  · {b['time']} {b['price']:.6g} +{b['rebound']:.1f}% ({b['reason']})"
                   for b in bounces[:4]]
        bounces_str = "🪜 Мини-отскоки в коррекции:\n" + "\n".join(b_lines)
    else:
        bounces_str = "Мини-отскоков не обнаружено"

    lines = [
        f"{icons.get(result,'📊')} <code>{symbol}</code> · {titles.get(result, result)}",
        "━━━━━━━━━━━━━━━━━━━",
        f"Сигнал: {sig['time']} · Зона: {zone_low:.6g}–{zone_high:.6g}",
        f"Надёжность: {sig['zq_score']}/10 · Рекомендация: {rec}%",
        f"Результат: <b>{sig['result_detail']}</b>",
        assess,
        "",
        travel_str,
        peak_str,
        vol_div,
        bounces_str,
        f"\n💎 <code>{coin}</code>",
    ]
    caption = "\n".join(l for l in lines if l)

    # График результата
    chart = None
    if k1 and len(k1) >= 20:
        try:
            # Строим с зоной
            levels_for_chart = [
                {"price": zone_high, "touches": 2, "type": "res", "tf": "zone"},
                {"price": zone_low,  "touches": 2, "type": "sup", "tf": "zone"},
            ]
            # Добавляем мини-отскоки как уровни
            for b in bounces[:3]:
                levels_for_chart.append({
                    "price": b["price"], "touches": 1, "type": "sup", "tf": "1m"
                })
            chart = build_inplay_chart(
                symbol, k1, zone_low, zone_high,
                [], [], [], levels_for_chart
            )
        except Exception as e:
            print(f"  [RESULT CHART ERR] {e}")

    if chart:
        send_photo(chart, caption)
    else:
        send_message(caption)
    print(f"  [RESULT] {symbol} → {result}")


def build_daily_stats():
    """Строит ежедневный отчёт по статистике сигналов."""
    with tracker_lock:
        all_sigs = list(signal_tracker.values())

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = all_sigs  # берём все за последние 24ч

    # Фильтруем за вчера
    from_ts = time.time() - 86400
    recent  = [s for s in all_sigs
               if time.mktime(datetime.strptime(s["time"], "%Y-%m-%d %H:%M").timetuple()) >= from_ts]

    if not recent:
        return "📊 <b>Статистика за 24ч</b>\nСигналов не было"

    total     = len(recent)
    bounced   = sum(1 for s in recent if s["result"] == "bounced")
    broke     = sum(1 for s in recent if s["result"] == "broke_zone")
    hit       = sum(1 for s in recent if s["result"] == "hit_zone")
    no_touch  = sum(1 for s in recent if s["result"] == "no_touch")
    pending   = sum(1 for s in recent if s["result"] is None)

    # По рекомендациям
    dont_enter = [s for s in recent if s["rec_pct"] == 0]
    de_correct = sum(1 for s in dont_enter if s["result"] in ("broke_zone", "no_touch"))

    enter_sigs = [s for s in recent if s["rec_pct"] > 0]
    en_correct = sum(1 for s in enter_sigs if s["result"] == "bounced")

    acc_de = round(de_correct / len(dont_enter) * 100) if dont_enter else 0
    acc_en = round(en_correct / len(enter_sigs) * 100) if enter_sigs else 0

    # По зонам
    by_score = {"опасно (0-3)": [], "слабая (4-5)": [], "умеренная (6-7)": [], "надёжная (8-10)": []}
    for s in recent:
        sc = s["zq_score"]
        if sc <= 3:   by_score["опасно (0-3)"].append(s)
        elif sc <= 5: by_score["слабая (4-5)"].append(s)
        elif sc <= 7: by_score["умеренная (6-7)"].append(s)
        else:         by_score["надёжная (8-10)"].append(s)

    lines = [
        f"📊 <b>Статистика сигналов за 24ч</b> · {today}",
        "━━━━━━━━━━━━━━━━━━━",
        f"Всего сигналов: <b>{total}</b>",
        f"  ✅ Отскок (зона держала): <b>{bounced}</b>",
        f"  🚨 Пробой зоны вниз:      <b>{broke}</b>",
        f"  📍 Цена в зоне:           <b>{hit}</b>",
        f"  ⏰ Не достигнута:         <b>{no_touch}</b>",
        f"  🔄 Ещё в работе:          <b>{pending}</b>",
        "",
        f"🎯 Точность рекомендаций:",
        f"  «Не заходить» ({len(dont_enter)} сигн.): <b>{acc_de}%</b> верно",
        f"  «Заходить» ({len(enter_sigs)} сигн.):    <b>{acc_en}%</b> верно",
        "",
        "📈 По надёжности зоны:",
    ]

    for label, sigs in by_score.items():
        if not sigs:
            continue
        b = sum(1 for s in sigs if s["result"] == "bounced")
        bk = sum(1 for s in sigs if s["result"] == "broke_zone")
        lines.append(f"  {label}: {len(sigs)} сигн · ✅{b} отскок · 🚨{bk} пробой")

    # Топ-3 лучших сигнала
    best = sorted([s for s in recent if s["result"] == "bounced"],
                  key=lambda x: x["zq_score"], reverse=True)[:3]
    if best:
        lines.append("")
        lines.append("🏆 Лучшие сигналы:")
        for s in best:
            lines.append(f"  {s['symbol']} · {s['zq_score']}/10 · {s['time']}")

    return "\n".join(lines)


def daily_stats_loop():
    """Отправляет статистику каждый день в 09:00."""
    print("  [STATS] Планировщик статистики запущен")
    while True:
        now    = datetime.now()
        target = now.replace(hour=9, minute=0, second=0, microsecond=0)
        if now >= target:
            # Следующее 09:00 завтра
            target = target.replace(year=target.year,
                                    month=target.month,
                                    day=target.day) 
            target = datetime(target.year, target.month, target.day, 9, 0, 0)
            target = target.replace(day=target.day + 1) if target <= now else target
            # Безопасный расчёт следующего дня
            from datetime import timedelta
            target = (now + timedelta(days=1)).replace(hour=9, minute=0,
                                                        second=0, microsecond=0)
        wait_sec = max(0, (target - now).total_seconds())
        print(f"  [STATS] Следующий отчёт через {wait_sec/3600:.1f}ч")
        time.sleep(wait_sec)

        # Проверяем результаты перед отчётом
        check_signal_results()
        msg = build_daily_stats()
        send_message(msg)
        print("  [STATS] 📊 Ежедневный отчёт отправлен")


def signal_monitor_loop():
    """Мониторит результаты сигналов каждые 2 минуты."""
    print("  [STATS] Мониторинг сигналов запущен")
    while True:
        time.sleep(120)
        try:
            check_signal_results()
        except Exception as e:
            print(f"  [STATS ERR] {e}")


# ─────────────────────────────────────────────
#  РАСХОЖДЕНИЕ ЦЕНЫ (MARK vs INDEX) + ФАНДИНГ
# ─────────────────────────────────────────────

DEVIATION_THRESHOLD = 0.3   # % расхождения mark vs index для алерта
DEVIATION_SCAN_SEC  = 30    # интервал проверки
deviation_cache: dict = {}  # { symbol: last_alert_ts }


def get_premium_index(symbol):
    """Получает mark price, index price, funding rate."""
    data = fetch(f"{BINANCE_BASE}/fapi/v1/premiumIndex", {"symbol": symbol})
    if not data:
        return None
    return {
        "mark":    float(data.get("markPrice", 0)),
        "index":   float(data.get("indexPrice", 0)),
        "funding": float(data.get("lastFundingRate", 0)),
    }


def build_deviation_chart(symbol, k1, mark, index):
    """График с линиями mark price и index price."""
    try:
        data   = k1[-60:]
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

        # VWAP
        vwap_vals = calc_vwap(data)
        ax1.plot(range(n), vwap_vals, color="#ff9800", lw=1.2,
                 ls="--", alpha=0.85, label="VWAP", zorder=6)

        # Mark price
        ax1.axhline(y=mark,  color="#ff4444", lw=1.5, ls="-",
                    label=f"Mark {mark:.6g}", zorder=7)
        # Index price
        ax1.axhline(y=index, color="#4488ff", lw=1.5, ls="--",
                    label=f"Index {index:.6g}", zorder=7)

        ax1.legend(loc="upper left", fontsize=7, facecolor="#161b22",
                   edgecolor="#30363d", labelcolor="white", framealpha=0.8)

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
        # Volume Profile
        draw_volume_profile(ax1, data)
        ax1.set_title(f"{symbol} · Расхождение Mark/Index",
                      color="#e6edf3", fontsize=12, fontweight="bold", pad=8)
        ax1.set_ylabel("Price", color="#8b949e", fontsize=9)
        ax2.set_ylabel("Vol USD", color="#8b949e", fontsize=9)
        plt.tight_layout(pad=1.2)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, facecolor="#0d1117")
        plt.close(fig); buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  [DEV CHART ERR {symbol}] {e}")
        return None


def deviation_scan_loop(symbols_ref):
    """Сканирует расхождение mark/index price каждые 30 сек."""
    print("  [DEV] Детектор расхождений запущен")
    while True:
        time.sleep(DEVIATION_SCAN_SEC)
        symbols = symbols_ref[0]
        if not symbols:
            continue

        def check_deviation(sym):
            try:
                pi = get_premium_index(sym)
                if not pi or pi["index"] == 0:
                    return
                mark   = pi["mark"]
                index  = pi["index"]
                fund   = pi["funding"]
                dev_pct = (mark - index) / index * 100

                if abs(dev_pct) < DEVIATION_THRESHOLD:
                    return

                # Дедупликация — не чаще раза в 15 мин на символ
                now = time.time()
                if now - deviation_cache.get(sym, 0) < 900:
                    return
                deviation_cache[sym] = now

                direction = "Выше" if dev_pct > 0 else "Ниже"
                coin = sym.replace("USDT", "")

                # Фандинг-интерпретация
                if fund > 0.001:
                    fund_str = f"🔴 {fund*100:.4f}% (лонги платят шортам — перегрев лонгов)"
                elif fund < -0.001:
                    fund_str = f"🟢 {fund*100:.4f}% (шорты платят лонгам — перегрев шортов)"
                else:
                    fund_str = f"⚪ {fund*100:.4f}% (нейтральный)"

                caption = (
                    f"📊 <code>{sym}</code> · расхождение цены\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"Тикер: <b>{sym}</b>\n"
                    f"Отклонение: <b>{abs(dev_pct):.2f}%</b> ({direction} индекса)\n"
                    f"Цена Index: <b>{index:.6g}</b>\n"
                    f"Цена Mark: <b>{mark:.6g}</b>\n"
                    f"Фандинг: {fund_str}\n"
                    f"━━━━━━━━━━━━━━━━━━━\n"
                    f"{'⚠️ Цена выше индекса — риск коррекции к индексу' if dev_pct > 0 else '⚠️ Цена ниже индекса — возможен отскок к индексу'}\n"
                    f"💎 <code>{coin}</code>"
                )

                k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                           {"symbol": sym, "interval": "1m", "limit": 60})
                chart = build_deviation_chart(sym, k1, mark, index) if k1 else None
                if chart:
                    send_photo(chart, caption)
                else:
                    send_message(caption)
                print(f"  [DEV] 📊 {sym} dev={dev_pct:+.2f}% fund={fund*100:.4f}%")

            except Exception as e:
                print(f"  [DEV ERR {sym}] {e}")

        # Проверяем только активные + инплей монеты (не все 300+)
        watched = set()
        with active_lock:
            watched.update(active_coins.keys())
        with inplay_lock:
            watched.update(inplay_coins.keys())

        if watched:
            with ThreadPoolExecutor(max_workers=10) as pool:
                list(pool.map(check_deviation, list(watched)))


# ─────────────────────────────────────────────
#  ЧАСОВОЙ ДАЙДЖЕСТ ИНПЛЕЙ МОНЕТ
# ─────────────────────────────────────────────

def build_inplay_digest_chart(symbol, k1):
    """Простой 1m график для дайджеста."""
    try:
        data   = k1[-60:]
        n      = len(data)
        opens  = [float(c[1]) for c in data]
        highs  = [float(c[2]) for c in data]
        lows   = [float(c[3]) for c in data]
        closes = [float(c[4]) for c in data]
        dvols  = [float(c[7]) for c in data]
        times  = [datetime.utcfromtimestamp(int(c[0])/1000) for c in data]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6),
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

        # VWAP
        vwap_vals = calc_vwap(data)
        ax1.plot(range(n), vwap_vals, color="#ff9800", lw=1.0,
                 ls="--", alpha=0.85, label="VWAP", zorder=6)
        ax1.legend(loc="upper left", fontsize=6, facecolor="#161b22",
                   edgecolor="#30363d", labelcolor="white", framealpha=0.7)

        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: fmt_usd(x)))
        ticks = list(range(0, n, max(1, n//5)))
        for ax in [ax1, ax2]:
            ax.set_xticks(ticks)
            ax.set_xticklabels([times[i].strftime("%H:%M") for i in ticks],
                                color="#8b949e", fontsize=7)
            ax.tick_params(colors="#8b949e", labelsize=7)
            ax.yaxis.set_tick_params(labelcolor="#8b949e")
            for sp in ax.spines.values(): sp.set_edgecolor("#30363d")
            ax.grid(color="#21262d", ls="--", lw=0.5)
            ax.set_xlim(-1, n)
        # Volume Profile
        draw_volume_profile(ax1, data, n_bins=30, alpha=0.2)
        ax1.set_title(f"{symbol} · 1m", color="#e6edf3",
                      fontsize=10, fontweight="bold", pad=6)
        plt.tight_layout(pad=1.0)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=100, facecolor="#0d1117")
        plt.close(fig); buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  [DIGEST CHART ERR {symbol}] {e}")
        return None


def hourly_inplay_digest():
    """Каждый час отправляет дайджест всех инплей монет."""
    print("  [DIGEST] Часовой дайджест запущен")
    while True:
        # Ждём до следующего часа :00
        now    = datetime.now()
        from datetime import timedelta
        next_h = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait   = (next_h - now).total_seconds()
        time.sleep(wait)

        with inplay_lock:
            snapshot = dict(inplay_coins)

        if not snapshot:
            send_message("📋 <b>Инплей дайджест</b>\nНет активных монет")
            continue

        # Заголовок дайджеста
        header = (f"📋 <b>Инплей дайджест</b> · {datetime.now().strftime('%H:%M')}\n"
                  f"Монет в наблюдении: <b>{len(snapshot)}</b>\n"
                  f"━━━━━━━━━━━━━━━━━━━")
        send_message(header)

        # По каждой монете — график + краткая сводка
        for sym, state in list(snapshot.items()):
            try:
                k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                           {"symbol": sym, "interval": "1m", "limit": 60})
                if not k1:
                    continue

                close  = float(k1[-1][4])
                # Изменение за день
                k1d = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                            {"symbol": sym, "interval": "1d", "limit": 2})
                day_open  = float(k1d[0][1]) if k1d and len(k1d) >= 1 else close
                day_chg   = (close - day_open) / day_open * 100 if day_open > 0 else 0

                # Объём за последний час
                k1h = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                            {"symbol": sym, "interval": "1h", "limit": 2})
                vol_1h = float(k1h[-2][7]) if k1h and len(k1h) >= 2 else 0

                zone_high = state.get("zone_high", 0)
                zone_low  = state.get("zone_low", 0)
                dist = (close - zone_high) / zone_high * 100 if zone_high else 0
                zq   = state.get("zq_score", "?")

                caption = (
                    f"<code>{sym}</code> · {day_chg:+.1f}% за день\n"
                    f"Цена: <b>{close:.6g}</b> · Объём 1ч: <b>{fmt_usd(vol_1h)}</b>\n"
                    f"Зона: {zone_low:.6g}–{zone_high:.6g} · До зоны: {dist:+.1f}%\n"
                    f"Надёжность: {zq}/10"
                )
                chart = build_inplay_digest_chart(sym, k1)
                if chart:
                    send_photo(chart, caption)
                else:
                    send_message(caption)
                time.sleep(1)  # не флудим

            except Exception as e:
                print(f"  [DIGEST ERR {sym}] {e}")

        print(f"  [DIGEST] Отправлен дайджест: {len(snapshot)} монет")


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

    # Инплей модуль в отдельном потоке
    threading.Thread(target=inplay_loop, args=(symbols_ref,), daemon=True).start()

    # Детектор псевдопампов в отдельном потоке
    threading.Thread(target=pseudo_loop, args=(symbols_ref,), daemon=True).start()

    # Детектор расхождений mark/index
    threading.Thread(target=deviation_scan_loop, args=(symbols_ref,), daemon=True).start()

    # Часовой дайджест инплей
    threading.Thread(target=hourly_inplay_digest, daemon=True).start()

    # Мониторинг результатов сигналов
    load_stats()
    threading.Thread(target=signal_monitor_loop, daemon=True).start()
    threading.Thread(target=daily_stats_loop, daemon=True).start()

    # Слой 1 — основной поток
    layer1_loop(symbols_ref)


if __name__ == "__main__":
    main()
