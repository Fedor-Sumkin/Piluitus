# clear_redis.py
import asyncio
from config import Settings
import redis.asyncio as aioredis

async def main():
    print("🧹 Подключение к Redis ОЗУ...")
    # Инициализируем клиент по вашим параметрам из config.py
    r = aioredis.Redis(
        host=getattr(Settings, "REDIS_HOST", "127.0.0.1"),
        port=getattr(Settings, "REDIS_PORT", 6379),
        decode_responses=True
    )
    
    try:
        # Полная очистка всех кэшей и дубликатов сделок
        await r.flushall()
        print("🎯 УСПЕХ: База данных Redis полностью очищена от старых дубликатов сессий!")
    except Exception as e:
        print(f"❌ Ошибка очистки: {e}")
    finally:
        await r.close()

if __name__ == "__main__":
    asyncio.run(main())
