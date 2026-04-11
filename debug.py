import requests

print("Тест подключения к Binance Futures...")
try:
    r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=10)
    print(f"HTTP статус: {r.status_code}")
    data = r.json()
    symbols = [s for s in data.get("symbols", []) if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"]
    print(f"Символов найдено: {len(symbols)}")
    if len(symbols) == 0:
        print("Все статусы:", set(s["status"] for s in data.get("symbols", [])))
        print("Все quoteAsset:", set(s["quoteAsset"] for s in data.get("symbols", [])[:20]))
except Exception as e:
    print(f"ОШИБКА: {e}")

print("\nТест Telegram...")
try:
    r2 = requests.get("https://api.telegram.org/bot8760663496:AAHGTxw7jcIq0u4USSfC_ANRi2oE47W547M/getMe", timeout=10)
    print(f"HTTP статус: {r2.status_code}")
    print(f"Ответ: {r2.json()}")
except Exception as e:
    print(f"ОШИБКА: {e}")
