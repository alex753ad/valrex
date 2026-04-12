"""
Crypto Screener v3 — Binance Futures → Telegram
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
TELEGRAM_BOT_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHANNEL_ID  = os.environ.get("TELEGRAM_CHANNEL_ID", "")

NATR_MIN             = 0.8
VOLUME_5M_MIN        = 400_000
RESEND_CHANGE_PCT    = 20
RESEND_MIN_INTERVAL  = 1000

PRICE_CHANGE_PCT     = 2.0
PRICE_RESEND_SEC     = 300

VOLUME_SPIKE_MULT    = 3.0
LEVEL_PROXIMITY_PCT  = 1.0
IMPULSE_MAX_MIN      = 30
MIN_SIGNAL_STARS     = 2

SCAN_INTERVAL_SEC    = 60
MAX_WORKERS          = 20

BINANCE_BASE = "https://fapi.binance.com"
# ─────────────────────────────────────────────

signal_cache_1: dict = {}
signal_cache_2: dict = {}
cache_lock = threading.Lock()
proxy_lock = threading.Lock()

# ── ПРОКСИ ────────────────────────────────────

PROXY_LIST = []
current_proxy = {"http": None, "https": None}


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
                lines = r.text.strip().split("\n")
                for line in lines:
                    line = line.strip()
                    if line and ":" in line:
                        proxies.append(line)
        except Exception as e:
            print(f"  [PROXY FETCH ERR] {e}")
    random.shuffle(proxies)
    PROXY_LIST = proxies[:100]
    print(f"  [PROXY] Загружено прокси: {len(PROXY_LIST)}")


def test_proxy(proxy_str):
    proxy = {"http": f"http://{proxy_str}", "https": f"http://{proxy_str}"}
    try:
        r = requests.get(f"{BINANCE_BASE}/fapi/v1/ping", proxies=proxy, timeout=8)
        return r.status_code == 200
    except Exception:
        return False


def find_working_proxy():
    global current_proxy
    print("  [PROXY] Ищу рабочий прокси для Binance...")
    for proxy_str in PROXY_LIST:
        if test_proxy(proxy_str):
            with proxy_lock:
                current_proxy = {
                    "http": f"http://{proxy_str}",
                    "https": f"http://{proxy_str}"
                }
            print(f"  [PROXY] ✅ Рабочий прокси: {proxy_str}")
            return True
    print("  [PROXY] ❌ Рабочий прокси не найден, работаю без прокси")
    with proxy_lock:
        current_proxy = {"http": None, "https": None}
    return False


def refresh_proxies():
    fetch_free_proxies()
    find_working_proxy()


# ── HTTP ──────────────────────────────────────

http = requests.Session()
http.headers.update({"User-Agent": "CryptoScreener/3.0"})


def fetch(url, params=None):
    with proxy_lock:
        proxy = dict(current_proxy)
    try:
        r = http.get(url, params=params, timeout=10, proxies=proxy)
        if r.status_code == 200:
            return r.json()
        elif r.status_code == 451:
            print("  [PROXY] Binance заблокировал IP, меняю прокси...")
            threading.Thread(target=find_working_proxy, daemon=True).start()
    except Exception as e:
        print(f"  [HTTP ERR] {e}")
    return None


def calc_atr(candles, period=14):
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h  = float(candles[i][2])
        l  = float(candles[i][3])
        pc = float(candles[i-1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period


def calc_natr(atr, close):
    return round(atr / close * 100, 2) if close else 0.0


def format_volume(v):
    if v >= 1_000_000:
        return f"{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"{v/1_000:.1f}k"
    return str(int(v))


def format_dollar(v):
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"${v/1_000:.0f}k"
    return f"${v:.0f}"


# ── УРОВНИ ────────────────────────────────────

def find_levels_for_tf(candles, tf_label, min_touches=2):
    highs  = [float(c[2]) for c in candles]
    lows   = [float(c[3]) for c in candles]
    pivot_highs = []
    pivot_lows  = []
    wing = 2
    for i in range(wing, len(candles) - wing):
        if highs[i] >= max(highs[i-wing:i] + highs[i+1:i+wing+1]):
            pivot_highs.append(highs[i])
        if lows[i] <= min(lows[i-wing:i] + lows[i+1:i+wing+1]):
            pivot_lows.append(lows[i])

    def cluster(points, pct=0.3):
        if not points:
            return []
        points = sorted(points)
        clusters = []
        group = [points[0]]
        for p in points[1:]:
            if (p - group[0]) / group[0] * 100 < pct:
                group.append(p)
            else:
                clusters.append(group)
                group = [p]
        clusters.append(group)
        return [(sum(g)/len(g), len(g)) for g in clusters if len(g) >= min_touches]

    levels = []
    for price, touches in cluster(pivot_highs):
        levels.append({"price": price, "touches": touches, "type": "res", "tf": tf_label})
    for price, touches in cluster(pivot_lows):
        levels.append({"price": price, "touches": touches, "type": "sup", "tf": tf_label})
    return levels


def find_all_levels(symbol, k1):
    all_levels = []
    all_levels += find_levels_for_tf(k1[-100:], "1m")
    k5 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
               {"symbol": symbol, "interval": "5m", "limit": 100})
    if k5:
        all_levels += find_levels_for_tf(k5, "5m")
    k15 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                {"symbol": symbol, "interval": "15m", "limit": 100})
    if k15:
        all_levels += find_levels_for_tf(k15, "15m")
    return all_levels


def nearest_level(close, levels):
    if not levels:
        return None, None
    best = min(levels, key=lambda l: abs(l["price"] - close))
    dist_pct = abs(best["price"] - close) / close * 100
    return best, round(dist_pct, 2)


def is_near_level(close, levels):
    level, dist = nearest_level(close, levels)
    if level is None:
        return False, None, None
    return dist <= LEVEL_PROXIMITY_PCT, level, dist


# ── ПАТТЕРНЫ ──────────────────────────────────

def detect_pattern(candles):
    if len(candles) < 3:
        return None, None
    c  = candles[-1]
    c1 = candles[-2]
    o,  h,  l,  cl  = float(c[1]),  float(c[2]),  float(c[3]),  float(c[4])
    o1, h1, l1, cl1 = float(c1[1]), float(c1[2]), float(c1[3]), float(c1[4])
    body = abs(cl - o)
    full_rng = h - l if h != l else 0.0001
    upper_shadow = h - max(o, cl)
    lower_shadow = min(o, cl) - l
    if lower_shadow >= body * 2 and lower_shadow >= upper_shadow * 2:
        return "🔨 Молот", "bull"
    if upper_shadow >= body * 2 and upper_shadow >= lower_shadow * 2:
        return "🔨 Перев. молот", "bear"
    if body / full_rng < 0.1:
        return "➕ Доджи", None
    if cl1 < o1 and cl > o and o <= cl1 and cl >= o1:
        return "🟢 Бычье поглощение", "bull"
    if cl1 > o1 and cl < o and o >= cl1 and cl <= o1:
        return "🔴 Медв. поглощение", "bear"
    if cl > o and body / full_rng > 0.7:
        return "📗 Сильная бычья", "bull"
    if cl < o and body / full_rng > 0.7:
        return "📕 Сильная медвежья", "bear"
    return None, None


def get_trend_15m(symbol):
    k = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
              {"symbol": symbol, "interval": "15m", "limit": 55})
    if not k or len(k) < 52:
        return "flat"
    closes = [float(c[4]) for c in k]
    def ema(data, period):
        kf = 2 / (period + 1)
        result = [sum(data[:period]) / period]
        for price in data[period:]:
            result.append(price * kf + result[-1] * (1 - kf))
        return result
    ema20 = ema(closes, 20)[-1]
    ema50 = ema(closes, 50)[-1]
    close = closes[-1]
    if close > ema20 > ema50:
        return "bull"
    if close < ema20 < ema50:
        return "bear"
    return "flat"


def volume_spike(k5_long):
    if not k5_long or len(k5_long) < 14:
        return 0, 0
    recent_vol = float(k5_long[-2][7])
    avg_vol    = sum(float(c[7]) for c in k5_long[-14:-2]) / 12
    mult = round(recent_vol / avg_vol, 1) if avg_vol > 0 else 0
    return recent_vol, mult


def calc_cvd(candles, lookback=20):
    data = candles[-lookback:]
    cvd = []
    running = 0
    for c in data:
        o, cl, vol = float(c[1]), float(c[4]), float(c[5])
        running += vol if cl >= o else -vol
        cvd.append(running)
    if len(cvd) < 5:
        return "neutral"
    recent = cvd[-3:]
    if recent[-1] > recent[-2] > recent[-3]:
        return "up"
    if recent[-1] < recent[-2] < recent[-3]:
        return "down"
    return "neutral"


def minutes_since_impulse(candles, threshold_pct=1.5):
    for i in range(len(candles) - 1, max(0, len(candles) - 60), -1):
        c = candles[i]
        o, cl = float(c[1]), float(c[4])
        if o > 0 and abs(cl - o) / o * 100 >= threshold_pct:
            return len(candles) - 1 - i
    return 999


def calc_stars(near_level, pattern_dir, signal_dir, trend, vol_mult, cvd, impulse_min):
    score = 1
    if near_level:
        score += 1
    if pattern_dir and pattern_dir == signal_dir:
        score += 1
    if (signal_dir == "bull" and trend == "bull") or \
       (signal_dir == "bear" and trend == "bear"):
        score += 0.5
    if vol_mult >= VOLUME_SPIKE_MULT:
        score += 1
    if (signal_dir == "bull" and cvd == "up") or \
       (signal_dir == "bear" and cvd == "down"):
        score += 0.5
    if impulse_min <= IMPULSE_MAX_MIN:
        score += 0.5
    return min(5, round(score))


def stars_str(n):
    return "⭐" * n + "☆" * (5 - n)


# ── ДЕДУПЛИКАЦИЯ ──────────────────────────────

def should_send_s1(symbol, natr):
    now = time.time()
    with cache_lock:
        prev = signal_cache_1.get(symbol)
        if prev is None:
            signal_cache_1[symbol] = {"natr": natr, "last_sent": now}
            return True
        time_ok    = (now - prev["last_sent"]) >= RESEND_MIN_INTERVAL
        change_pct = abs(natr - prev["natr"]) / prev["natr"] * 100 if prev["natr"] else 100
        if time_ok or change_pct >= RESEND_CHANGE_PCT:
            signal_cache_1[symbol] = {"natr": natr, "last_sent": now}
            return True
    return False


def should_send_s2(symbol):
    now = time.time()
    with cache_lock:
        last = signal_cache_2.get(symbol, 0)
        if (now - last) >= PRICE_RESEND_SEC:
            signal_cache_2[symbol] = now
            return True
    return False


# ── ГРАФИК ────────────────────────────────────

TF_COLORS = {
    "sup": {"1m": "#00e5ff", "5m": "#00ff99", "15m": "#ffff00"},
    "res": {"1m": "#ff4444", "5m": "#ff8800", "15m": "#ff44ff"}
}


def build_chart(symbol, candles, levels=None):
    try:
        data = candles[-60:]
        n = len(data)
        opens   = [float(c[1]) for c in data]
        highs   = [float(c[2]) for c in data]
        lows    = [float(c[3]) for c in data]
        closes  = [float(c[4]) for c in data]
        dollar_vols = [float(c[7]) for c in data]
        times   = [datetime.utcfromtimestamp(int(c[0]) / 1000) for c in data]

        fig, (ax1, ax2) = plt.subplots(
            2, 1, figsize=(13, 8),
            gridspec_kw={"height_ratios": [3, 1]},
            facecolor="#0d1117"
        )
        ax1.set_facecolor("#0d1117")
        ax2.set_facecolor("#0d1117")

        w = 0.6
        for i in range(n):
            color = "#26a69a" if closes[i] >= opens[i] else "#ef5350"
            ax1.plot([i, i], [lows[i], highs[i]], color=color, linewidth=0.8)
            body_h = abs(closes[i] - opens[i]) or (highs[i] - lows[i]) * 0.01
            body_y = min(opens[i], closes[i])
            ax1.add_patch(Rectangle((i - w/2, body_y), w, body_h,
                                     facecolor=color, edgecolor=color))

        if levels:
            price_min = min(lows)
            price_max = max(highs)
            margin = (price_max - price_min) * 0.15
            visible = [lv for lv in levels
                       if price_min - margin <= lv["price"] <= price_max + margin]
            seen = []
            deduped = []
            for lv in sorted(visible, key=lambda x: x["price"]):
                too_close = any(abs(lv["price"] - s) / s * 100 < 0.2 for s in seen)
                if not too_close:
                    deduped.append(lv)
                    seen.append(lv["price"])
            for lv in deduped[:10]:
                tf  = lv["tf"]
                typ = lv["type"]
                color = TF_COLORS.get(typ, {}).get(tf, "#ffffff")
                lw    = 1.5 if tf == "15m" else 1.0
                ls    = "-" if tf == "15m" else "--"
                ax1.axhline(y=lv["price"], color=color, linewidth=lw,
                            linestyle=ls, alpha=0.9, zorder=5)
                label = f" {lv['price']:.6g}  [{tf}] ×{lv['touches']}"
                ax1.text(n - 0.5, lv["price"], label, color=color, fontsize=7,
                         va="center", fontweight="bold",
                         bbox=dict(facecolor="#0d1117", alpha=0.6, pad=1, edgecolor="none"))

            from matplotlib.lines import Line2D
            legend_items = []
            for tf, tc in [("1m", "#00e5ff"), ("5m", "#00ff99"), ("15m", "#ffff00")]:
                legend_items.append(Line2D([0], [0], color=tc, lw=1.2, label=f"Sup {tf}"))
            for tf, tc in [("1m", "#ff4444"), ("5m", "#ff8800"), ("15m", "#ff44ff")]:
                legend_items.append(Line2D([0], [0], color=tc, lw=1.2, label=f"Res {tf}"))
            ax1.legend(handles=legend_items, loc="upper left", fontsize=6, ncol=2,
                       facecolor="#161b22", edgecolor="#30363d",
                       labelcolor="white", framealpha=0.8)

        for i in range(n):
            color = "#26a69a" if closes[i] >= opens[i] else "#ef5350"
            ax2.bar(i, dollar_vols[i], color=color, width=w, alpha=0.85)

        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: format_dollar(x)))
        tick_idx = list(range(0, n, max(1, n // 6)))
        for ax in [ax1, ax2]:
            ax.set_xticks(tick_idx)
            ax.set_xticklabels([times[i].strftime("%H:%M") for i in tick_idx],
                                color="#8b949e", fontsize=8)
            ax.tick_params(colors="#8b949e", labelsize=8)
            ax.yaxis.set_tick_params(labelcolor="#8b949e")
            for spine in ax.spines.values():
                spine.set_edgecolor("#30363d")
            ax.grid(color="#21262d", linestyle="--", linewidth=0.5)
            ax.set_xlim(-1, n)

        ax1.set_title(f"{symbol} (1m свечи)", color="#e6edf3",
                      fontsize=13, fontweight="bold", pad=10)
        ax1.set_ylabel("Price", color="#8b949e", fontsize=9)
        ax2.set_ylabel("Volume (USD)", color="#8b949e", fontsize=9)

        plt.tight_layout(pad=1.2)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, facecolor="#0d1117")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  [CHART ERR {symbol}] {e}")
        return None


# ── ПОДПИСИ ───────────────────────────────────

def trend_emoji(t):
    return {"bull": "▲ бычий", "bear": "▼ медвежий", "flat": "➡ флэт"}.get(t, "➡ флэт")


def cvd_emoji(c):
    return {"up": "↑ растёт", "down": "↓ падает", "neutral": "→ нейтрально"}.get(c, "→")


def level_line(level, dist_pct):
    if not level or dist_pct is None:
        return None
    ltype = "поддержка" if level["type"] == "sup" else "сопротивление"
    return (f"📍 Уровень [{level['tf']}] ({ltype}): "
            f"<b>{level['price']:.6g}</b> "
            f"(×{level['touches']}) — <b>{dist_pct}%</b>")


def build_caption(header, symbol, main_line, natr, volume_5m, vol_mult,
                  level, dist_pct, pattern_name, trend, cvd, impulse_min, stars):
    coin = symbol.replace("USDT", "")
    lines = [
        f"{header}  {stars_str(stars)}",
        "━━━━━━━━━━━━━━━",
        f"🟨 Тикер: <b>{symbol}</b>",
        main_line,
        f"📈 Natr (1 мин): <b>{natr}%</b>",
        f"💰 Объём (5 мин): <b>{format_volume(volume_5m)}</b>",
    ]
    if vol_mult >= 1.5:
        lines.append(f"🔥 Всплеск объёма: <b>×{vol_mult}</b> от среднего")
    ll = level_line(level, dist_pct)
    if ll:
        lines.append(ll)
    if pattern_name:
        lines.append(f"🕯 Паттерн: <b>{pattern_name}</b>")
    lines.append(f"📊 Тренд 15m: <b>{trend_emoji(trend)}</b>")
    lines.append(f"📉 CVD дельта: <b>{cvd_emoji(cvd)}</b>")
    if impulse_min < 999:
        lines.append(f"⏱ Импульс: <b>{impulse_min} мин назад</b>")
    lines.append(f"\n💎 Монета: <b>{coin}</b>")
    return "\n".join(lines)


# ── TELEGRAM ──────────────────────────────────

def send_photo(image_bytes, caption):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        r = requests.post(url, data={
            "chat_id": TELEGRAM_CHANNEL_ID,
            "caption": caption,
            "parse_mode": "HTML"
        }, files={"photo": ("chart.png", image_bytes, "image/png")}, timeout=20)
        if not r.json().get("ok"):
            print(f"  [TG ERROR] {r.json()}")
    except Exception as e:
        print(f"  [TG EXCEPTION] {e}")


def send_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={
            "chat_id": TELEGRAM_CHANNEL_ID,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        if not r.json().get("ok"):
            print(f"  [TG ERROR] {r.json()}")
    except Exception as e:
        print(f"  [TG EXCEPTION] {e}")


# ── СКАНИРОВАНИЕ ─────────────────────────────

def get_symbols():
    data = fetch(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    if not data:
        return []
    return [
        s["symbol"] for s in data["symbols"]
        if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"
    ]


def scan_symbol(symbol):
    sent = 0
    try:
        k1 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                   {"symbol": symbol, "interval": "1m", "limit": 120})
        if not k1 or len(k1) < 20:
            return 0

        k5 = fetch(f"{BINANCE_BASE}/fapi/v1/klines",
                   {"symbol": symbol, "interval": "5m", "limit": 14})

        close      = float(k1[-1][4])
        prev_close = float(k1[-2][4])
        atr        = calc_atr(k1, 14)
        natr       = calc_natr(atr, close)
        price_chg  = (close - prev_close) / prev_close * 100 if prev_close else 0

        volume_5m, vol_mult = volume_spike(k5)

        levels            = find_all_levels(symbol, k1)
        near, level, dist = is_near_level(close, levels)
        pattern_name, pat_dir = detect_pattern(k1)
        trend             = get_trend_15m(symbol)
        cvd               = calc_cvd(k1)
        impulse_min       = minutes_since_impulse(k1)

        if abs(price_chg) >= PRICE_CHANGE_PCT and should_send_s2(symbol):
            sig_dir = "bull" if price_chg > 0 else "bear"
            stars   = calc_stars(near, pat_dir, sig_dir, trend, vol_mult, cvd, impulse_min)
            if stars >= MIN_SIGNAL_STARS:
                direction = "🚀" if price_chg > 0 else "💥"
                sign = "+" if price_chg > 0 else ""
                main_line = f"{direction} Изменение цены: <b><u>{sign}{price_chg:.2f}%</u></b>"
                cap = build_caption(
                    "⚡️ <b>Резкое движение цены</b>", symbol, main_line,
                    natr, volume_5m, vol_mult, level, dist,
                    pattern_name, trend, cvd, impulse_min, stars
                )
                print(f"  ⚡ ЦЕНА: {symbol} | Δ={price_chg:+.2f}% | ⭐{stars}")
                chart = build_chart(symbol, k1, levels)
                if chart:
                    send_photo(chart, cap)
                else:
                    send_message(cap)
                sent += 1

        if natr >= NATR_MIN and volume_5m >= VOLUME_5M_MIN and should_send_s1(symbol, natr):
            sig_dir = "bull" if float(k1[-1][4]) >= float(k1[-1][1]) else "bear"
            stars   = calc_stars(near, pat_dir, sig_dir, trend, vol_mult, cvd, impulse_min)
            if stars >= MIN_SIGNAL_STARS:
                main_line = f"📈 Natr (1 мин): <b><u>{natr}%</u></b>"
                cap = build_caption(
                    "📊 <b>NATR-сигнал</b>", symbol, main_line,
                    natr, volume_5m, vol_mult, level, dist,
                    pattern_name, trend, cvd, impulse_min, stars
                )
                print(f"  ✅ NATR: {symbol} | {natr}% | VOL={format_volume(volume_5m)} | ⭐{stars}")
                chart = build_chart(symbol, k1, levels)
                if chart:
                    send_photo(chart, cap)
                else:
                    send_message(cap)
                sent += 1

    except Exception as e:
        print(f"  [ERR {symbol}] {e}")
    return sent


def main_loop():
    print("🚀 Скринер v3 запущен. Ожидание сигналов...\n")

    # Загружаем прокси при старте
    refresh_proxies()

    # Обновляем прокси каждые 30 минут в фоне
    def proxy_refresher():
        while True:
            time.sleep(1800)
            refresh_proxies()

    threading.Thread(target=proxy_refresher, daemon=True).start()

    while True:
        start = time.time()
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] Сканирование...")

        symbols = get_symbols()
        print(f"  Найдено символов: {len(symbols)}")

        signals = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(scan_symbol, s): s for s in symbols}
            for f in as_completed(futures):
                try:
                    signals += f.result()
                except Exception:
                    pass

        elapsed = time.time() - start
        print(f"  Сигналов: {signals} | Время: {elapsed:.1f}s\n")
        time.sleep(max(0, SCAN_INTERVAL_SEC - elapsed))


if __name__ == "__main__":
    main_loop()
