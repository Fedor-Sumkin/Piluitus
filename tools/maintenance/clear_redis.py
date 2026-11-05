# Bootstrap: allow running this utility from any working directory.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Safe Redis maintenance utility. Clears only the selected Redis database.
import argparse
import asyncio
from config import Settings
import redis.asyncio as aioredis

async def main(confirm: bool = False):
    host = getattr(Settings, "REDIS_HOST", "127.0.0.1")
    port = getattr(Settings, "REDIS_PORT", 6379)
    db = getattr(Settings, "REDIS_DB", 0)
    print(f"Redis target: {host}:{port}, database={db}")
    if not confirm:
        print("Операция не выполнена. Для очистки только выбранной БД повторите запуск с --confirm.")
        return
    client = aioredis.Redis(host=host, port=port, db=db, decode_responses=True)
    try:
        await client.flushdb()
        print(f"База Redis №{db} очищена. Остальные базы не затронуты.")
    except Exception as exc:
        print(f"Ошибка очистки Redis: {exc}")
        raise
    finally:
        await client.aclose()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Очистка выбранной базы Redis PILUITUS")
    parser.add_argument("--confirm", action="store_true", help="подтвердить очистку выбранной БД (FLUSHDB)")
    asyncio.run(main(confirm=parser.parse_args().confirm))
