# Bootstrap: allow running this utility from any working directory.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import os
import json
import glob
import pandas as pd
from typing import List, Dict, Any

# --- НАСТРОЙКИ МОДЕРНИЗАЦИИ ---
CSV_DIR = str(PROJECT_ROOT / "historical_ticks")          
RAG_OUTPUT = str(PROJECT_ROOT / "ai_market_knowledge.json") 
MULTIPLIER_THRESHOLD = 7.0            # ИСПРАВЛЕНО: Жесткий фильтр (Объем выше среднего в 7+ раз)
WINDOW_SIZE = 20                      

def extract_anomalies_from_file(file_path: str) -> List[Dict[str, Any]]:
    anomalies = []
    ticker = os.path.basename(file_path).split(".")[0]
    
    try:
        df = pd.read_csv(file_path)
        if df.empty or len(df) < WINDOW_SIZE:
            return []

        # Расчет среднего объема строго назад
        df['avg_vol'] = df['vol'].shift(1).rolling(window=WINDOW_SIZE).mean()
        df['avg_vol'] = df['avg_vol'].fillna(df['vol'])

        # Сильнейший математический фильтр крупных фондов
        anomaly_mask = df['vol'] > (df['avg_vol'] * MULTIPLIER_THRESHOLD)
        df_anomalies = df[anomaly_mask].copy()

        if df_anomalies.empty:
            return []

        anomaly_list = df_anomalies.to_dict(orient="records")

        for row in anomaly_list:
            current_date = row['date']
            current_time = row['time']
            
            orig_idx_series = df[(df['date'] == current_date) & (df['time'] == current_time)].index
            if orig_idx_series.empty:
                continue
                
            orig_idx = int(orig_idx_series[0])
            
            if orig_idx > 0:
                prev_close = float(df.iloc[orig_idx - 1]['close'])
            else:
                prev_close = float(row['close'])
            
            price_change_pct = ((float(row['close']) - prev_close) / prev_close) * 100
            
            if price_change_pct > 0.1:
                direction = "PUMP"
            elif price_change_pct < -0.1:
                direction = "DUMP"
            else:
                direction = "ABSORPTION"

            # УЛЬТРА-ПЛОТНЫЙ СИСТЕМНЫЙ ФОРМАТ ПОД 4096 КОНТЕКСТ QWEN
            pattern = {
                "ticker": ticker,
                "date": str(int(row['date'])),
                "time": str(int(row['time'])).zfill(6),
                "context": f"HFT_SPIKE: {round(float(row['vol']/row['avg_vol']), 1)}x | Price: {round(price_change_pct, 2)}% [{direction}]"
            }
            anomalies.append(pattern)

    except Exception as e:
        print(f"💥 Ошибка файла {file_path}: {str(e)}")
        return []
                
    return anomalies

def main():
    print("🚀 Запуск модернизированного квант-экстрактора History-to-RAG...")
    
    search_path = os.path.join(CSV_DIR, "*.csv")
    file_list = glob.glob(search_path)
    
    if not file_list:
        print(f"❌ Ошибка: В папке '{CSV_DIR}' не найдено .csv файлов!")
        return

    print(f"Найдено файлов для анализа: {len(file_list)}")
    all_extracted_cases = []
    processed_count = 0

    for file_path in file_list:
        cases = extract_anomalies_from_file(file_path)
        if cases:
            all_extracted_cases.extend(cases)
        processed_count += 1
        
        if processed_count % 10 == 0 or processed_count == len(file_list):
            print(f"⏳ Обработано {processed_count}/{len(file_list)} файлов... Извлечено чистых аномалий: {len(all_extracted_cases)}")

    if all_extracted_cases:
        existing_rules = []
        if os.path.exists(RAG_OUTPUT):
            try:
                with open(RAG_OUTPUT, "r", encoding="utf-8") as f:
                    existing_rules = json.load(f)
                    # Фильтруем, оставляя только 330 дистиллированных ИИ-правил из книг
                    existing_rules = [item for item in existing_rules if "ticker" not in item]
            except Exception:
                existing_rules = []

        # Безопасное слияние: Книжные правила (база) + новые чистые аномалии Мосбиржи
        combined_data = existing_rules + all_extracted_cases
        
        with open(RAG_OUTPUT, "w", encoding="utf-8") as f:
            json.dump(combined_data, f, ensure_ascii=False, indent=4)
            
        print(f"\n🎯 УСПЕХ! Извлечено {len(all_extracted_cases)} высококонцентрированных институциональных аномалий.")
        print(f"Файл {RAG_OUTPUT} успешно обновлен. Общее количество объектов в RAG: {len(combined_data)}")
    else:
        print("\n⚠️ Внимание: Крупных аномалий не обнаружено. Проверьте исторические файлы.")

if __name__ == "__main__":
    main()
