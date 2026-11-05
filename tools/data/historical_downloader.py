# Bootstrap: allow running this utility from any working directory.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import os
import asyncio
import httpx
import pandas as pd
from datetime import datetime, timedelta, UTC
from dotenv import load_dotenv

# --- ЗАГРУЗКА КОНФИГУРАЦИИ ---
load_dotenv()
API_TOKEN = os.getenv("FINAM_TOKEN")

BASE_URL = "https://api.finam.ru"
CSV_DIR = str(PROJECT_ROOT / "data" / "historical_ticks")
os.makedirs(CSV_DIR, exist_ok=True)

DAYS_BACK = 90
STEP_DAYS = 7

# Полный список 100 акций за вычетом делистинга
TICKERS_LIST = [
    # 1-й Эшелон (Голубые фишки)
    "SBER@MISX", "SBERP@MISX", "GAZP@MISX", "LKOH@MISX", "NVTK@MISX", 
    "YDEX@MISX", "ROSN@MISX", "VTBR@MISX", "AFLT@MISX", "MAGN@MISX", "CHMF@MISX", 
    "NLMK@MISX", "MOEX@MISX", "ALRS@MISX", "MTSS@MISX", "MGNT@MISX", 
    "TATN@MISX", "SNGS@MISX", "SNGSP@MISX", "PLZL@MISX", "HYDR@MISX", 
    "FEES@MISX", "CBOM@MISX", "IRAO@MISX", "PHOR@MISX", "PIKK@MISX", 
    "TRNFP@MISX", "TATNP@MISX",
    
    # 2-й Эшелон
    "AFKS@MISX", "RUAL@MISX", "GCHE@MISX", "LSRG@MISX", "SMLT@MISX", 
    "MSNG@MISX", "BANEP@MISX", "BSPB@MISX", "RENI@MISX", 
    "UPRO@MISX", "SOFL@MISX", "POSI@MISX", "RNFT@MISX", 
    "UWGN@MISX", "VKCO@MISX", "HEAD@MISX", "OZON@MISX", "APRI@MISX", 
    "MVID@MISX", "LENT@MISX", "OKEY@MISX",  "AGRO@MISX", 
    "MDMG@MISX", "WUSH@MISX", "RASP@MISX", "CARM@MISX", "LEAS@MISX", 
    "DELI@MISX", "SVCB@MISX", "SFIN@MISX", "MTLR@MISX", "MTLRP@MISX", 
    "FESH@MISX", "NMTP@MISX", "FLOT@MISX", "GECO@MISX", 
    "ABIO@MISX", "ALNU@MISX", "APTK@MISX", "BLNG@MISX", "DVEC@MISX", 
    "ELFV@MISX", "ENPG@MISX", "GTRK@MISX", "IRKT@MISX", "KAZT@MISX", 
    "KAZTP@MISX", "KBSB@MISX", "KROT@MISX", "KUBE@MISX", 
    "MAGE@MISX", "MAGEP@MISX", "MGTS@MISX", "MGTSP@MISX", 
    "NKHP@MISX", "NKNC@MISX", "NKNCP@MISX", "TGKA@MISX", "TGKB@MISX", 
    "TGKBP@MISX", "TTLK@MISX", "VJGZ@MISX", "VJGZP@MISX"
]

# Глобальный контейнер для удержания токена сессии
CURRENT_JWT = [None]

async def get_jwt_token(client: httpx.AsyncClient) -> str:
    """Получает или обновляет временный JWT токен сессии"""
    url = f"{BASE_URL}/v1/sessions"
    payload = {"secret": API_TOKEN}
    headers = {
        "User-Agent": "PiluitusTradingBot/2.0",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    print("🔑 [TOKEN_REFRESH] Запрос обновленного JWT-токена сессии Финам...")
    response = await client.post(url, json=payload, headers=headers, timeout=15)
    
    if response.status_code == 200:
        data = response.json()
        jwt_token = data.get("token") or data.get("jwt") or data.get("data", {}).get("token")
        if jwt_token:
            CURRENT_JWT[0] = jwt_token
            print("✅ Новый JWT-токен успешно внедрен сессия продлена еще на 10 минут.")
            return jwt_token
        raise ValueError(f"Не удалось десериализовать токен из ответа: {data}")
    else:
        raise ConnectionError(f"Ошибка продления сессии {response.status_code}: {response.text}")

async def download_ticker_3months(client: httpx.AsyncClient, symbol: str):
    """Скачивает минутные бары за 3 месяца с автоматическим рефрешем токена и пропуском существующих файлов"""
    clean_symbol = symbol.split("@")[0]
    file_path = os.path.join(CSV_DIR, f"{clean_symbol}.csv")
    
    # --- ВНЕДРЕНА ЛОГИКА УМНОГО ПРОПУСКА (SMART SKIP) ---
    if os.path.exists(file_path) and os.path.getsize(file_path) > 100:
        print(f"⏭️  [PROPUSK] Файл для {clean_symbol} уже существует и содержит данные. Переходим к следующему.")
        return

    url = f"{BASE_URL}/v1/instruments/{symbol}/bars"
    global_end_time = datetime.now(UTC)
    global_start_time = global_end_time - timedelta(days=DAYS_BACK)
    
    current_end = global_end_time
    all_records = []
    
    print(f"📥 Сбор истории за 3 месяца для {symbol}...")
    
    while current_end > global_start_time:
        current_start = current_end - timedelta(days=STEP_DAYS)
        if current_start < global_start_time:
            current_start = global_start_time
            
        params = {
            "timeframe": "TIME_FRAME_M1",
            "interval.start_time": current_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval.end_time": current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        headers = {
            "Authorization": f"Bearer {CURRENT_JWT[0]}",
            "Accept": "application/json",
            "User-Agent": "PiluitusTradingBot/2.0"
        }
        
        try:
            response = await client.get(url, params=params, headers=headers, timeout=20)
            
            if response.status_code == 200:
                data = response.json()
                bars = data.get("bars", []) or data.get("data", {}).get("bars", [])
                
                for bar in bars:
                    dt_parsed = pd.to_datetime(bar.get("timestamp") or bar.get("time"))
                    
                    o = bar["open"]["value"] if isinstance(bar["open"], dict) else bar["open"]
                    h = bar["high"]["value"] if isinstance(bar["high"], dict) else bar["high"]
                    l = bar["low"]["value"] if isinstance(bar["low"], dict) else bar["low"]
                    c = bar["close"]["value"] if isinstance(bar["close"], dict) else bar["close"]
                    v = bar["volume"]["value"] if isinstance(bar["volume"], dict) else bar["volume"]
                    
                    all_records.append({
                        "date": dt_parsed.strftime("%Y%m%d"),
                        "time": dt_parsed.strftime("%H%M%S"),
                        "open": float(o),
                        "high": float(h),
                        "low": float(l),
                        "close": float(c),
                        "vol": int(float(v)),
                        "raw_dt": dt_parsed
                    })
            elif response.status_code == 401:
                print("⏳ Сессия истекла (401). Запуск экстренного перевыпуска токена...")
                await get_jwt_token(client)
                continue
                
            elif response.status_code == 429:
                print("⏳ Превышен лимит запросов. Ожидание 15 секунд...")
                await asyncio.sleep(15)
                continue
            else:
                print(f"❌ Ошибка окна для {symbol}: Статус {response.status_code}.")
                
        except Exception as e:
            print(f"💥 Исключение в окне для {symbol}: {str(e)}")
            
        current_end = current_start
        await asyncio.sleep(0.3)
        
    if all_records:
        df = pd.DataFrame(all_records)
        df = df.sort_values(by="raw_dt").drop_duplicates(subset=["date", "time"])
        df = df.drop(columns=["raw_dt"])
        
        df.to_csv(file_path, index=False)
        print(f"✅ ИСТОРИЯ ЗАПИСАНА: {file_path} (Суммарно баров: {len(df)})")
    else:
        print(f"⚠️ Не удалось собрать историю для {symbol}")

async def main():
    if not API_TOKEN:
        print("❌ КРИТИЧЕСКАЯ ОШИБКА: FINAM_TOKEN не найден в .env!")
        return
        
    async with httpx.AsyncClient(http2=True, follow_redirects=True) as client:
        try:
            await get_jwt_token(client)
            print("🚀 Первичная авторизация пройдена. Начинается потоковый сбор пула истории...")
            
            for ticker in TICKERS_LIST:
                await download_ticker_3months(client, ticker)
                await asyncio.sleep(0.5)
                
            print("\n🎉 КОНВЕЙЕР ЗАВЕРШЕН: Все недостающие файлы за 3 месяца успешно скачаны на диск!")
            
        except Exception as e:
            print(f"❌ Критический сбой конвейера: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
