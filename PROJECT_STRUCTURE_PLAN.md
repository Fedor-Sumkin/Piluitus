# План реорганизации структуры PILUITUS

Статус: подготовка на отдельной ветке `refactor/project-structure`.
База ветки: `fix/bbo-sltp-top15-terminal`.
Назначение: структурная реорганизация без изменения торговой логики.

## Обязательные ограничения

- Не менять алгоритм отбора 88 инструментов и Top-15.
- Не менять условия допуска сигналов, ATR, VWAP, spread, size, BBO TTL и семантику сторон исполнения.
- Не менять риск-лимиты, размер позиции, PAPER-учёт, SL/TP и Finam schedule guards.
- Не удалять файлы только на основании отсутствия очевидного импорта.
- Не переносить .env, секреты, пользовательские данные и локальное виртуальное окружение в Git.
- Сохранить запуск основного приложения из корня: `python main.py`.
- Для служебных скриптов предусмотреть запуск из корня и из их собственной папки.
- Все пути к данным/логам/шаблонам сделать независимыми от текущей рабочей директории, где это безопасно.

## Предварительная целевая структура

```text
Piluitus/
├── main.py                       # Совместимая главная точка входа
├── config.py                     # Общая конфигурация и PROJECT_ROOT
├── requirements.txt
├── .env.example
├── piluitus/
│   ├── __init__.py
│   ├── core/                     # Координация, запуск, восстановление кандидатов
│   ├── exchange/                 # Finam API и рыночные данные
│   ├── strategy/                 # Скринер, фильтры, паттерны и signal guards
│   ├── ai/                       # LLM, RAG, знания и новости
│   ├── risk/                     # RiskManager и risk limits
│   ├── execution/                # Исполнение заявок и PAPER engine
│   ├── portfolio/                # PnL, метрики, экспорт
│   ├── storage/                  # DBManager и адаптеры хранилища
│   └── monitoring/               # Логирование и диагностика
├── interface/
│   ├── tg_bot.py
│   └── templates/
├── tools/
│   ├── maintenance/               # clear_redis, clean_rag
│   ├── data/                      # historical_downloader, history_to_rag
│   ├── knowledge/                 # distiller, merge/import scripts
│   └── patches/                   # install/rollback local patch
├── data/
│   ├── historical_ticks/
│   ├── knowledge/
│   └── exports/
├── logs/                          # runtime logs; ignored by Git
├── live_market_logs/              # snapshots; ignored by Git
└── tests/
```

Это целевая схема группировки. Точные подпапки и распределение каждого модуля будут утверждены после полного аудита импортов, динамических импортов, путей и точек запуска.

## Предварительная карта модулей

### Торговое ядро и рыночные данные
- `main.py` — оставить в корне как стабильную точку запуска.
- `finam_client.py` — exchange.
- `screener.py`, `screener_funnel.py`, `signal_guard.py`, `minute_bar_patterns.py` — strategy.
- `candidate_recovery.py` — core/strategy helper.
- `risk_manager.py`, `risk_limits.py` — risk.
- `execution_layer.py` — execution.
- `db_manager.py` — storage.
- `ai_engine.py`, `vllm_client.py`, `dashboard_verdicts.py`, `rag_prompt_utils.py`, `knowledge_parser.py` — ai (dashboard helper may instead belong to interface).
- `news_parser.py` — ai/news.
- `market_logger.py` — monitoring.
- `portfolio_metrics.py`, `pnl_exporter.py` — portfolio.
- `tg_bot.py`, `templates/` — interface.

### Служебные инструменты
- `clear_redis.py` — tools/maintenance. Важно: сейчас вызывает Redis FLUSHALL, то есть очищает ВСЕ базы Redis на выбранном сервере; это destructive utility, запуск должен требовать явного подтверждения и показывать адрес/DB.
- `clean_rag.py` — tools/maintenance. Перезаписывает knowledge JSON и обновляет RAG cache.
- `historical_downloader.py` — tools/data.
- `history_to_rag.py` — tools/data или tools/knowledge (уточнить назначение по выходному формату).
- `rag_ai_distiller.py`, `merge_rules.py`, `import_knowledge/inject_txt_reglement.py` — tools/knowledge.
- `install_local_patch.py`, `rollback_local_patch.py` — tools/patches.
- `evolution_agent.py` — требуется отдельная проверка: файл содержит класс `BaseExecutionEngine`, похожий на устаревшую/дублирующую реализацию `execution_layer.py`. До проверки ссылок и истории не удалять и не перемещать в архив автоматически.
- `clear_redis.py`, `clean_rag.py`, `merge_rules.py`, `history_to_rag.py` и `historical_downloader.py` должны получить надёжные пути от `PROJECT_ROOT` и bootstrap для импорта конфигурации при запуске из своей папки.

### Данные и ресурсы
- `historical_ticks/` → `data/historical_ticks/`.
- `ai_market_knowledge.json`, `ai_market_news.json`, `purified_book_rules.json`, `piluitus_fallback_aof.jsonl`, `trading_journal.json` → после проверки потребителей в `data/knowledge/`, `data/cache/` или `data/state/`.
- `import_knowledge/` → `data/knowledge/sources/`; исполняемые импортёры отделить в `tools/knowledge/`.
- `templates/` → `interface/templates/`; проверить все ссылки FastAPI/Jinja и StaticFiles.
- `trading_logs/`, `live_market_logs/`, отчёты и runtime state не должны коммититься.

## Обнаруженные риски, требующие решения при реализации

1. В проекте много абсолютных импортов вида `from config import ...`, `from db_manager import ...`; простое перемещение модулей сломает запуск. Нужны пакетные импорты и временные root-compatibility shims либо синхронный переход всех импортов.
2. `main.py` содержит пути к шаблонам и runtime-файлам; их нельзя менять механически.
3. `knowledge_parser.py` сейчас вычисляет путь к `ai_market_knowledge.json` относительно расположения самого файла. При переносе путь изменится.
4. Часть инструментов использует относительные пути от текущей директории, а `rag_ai_distiller.py` содержит абсолютный Windows-путь `C:\\projects\\Piluitus\\import_knowledge`.
5. `tg_bot.py` создаёт глобальные объекты и получает ссылки на брокер/движок из `main.py`; при переносе важно сохранить идентичность модуля, чтобы не создать вторые независимые глобальные состояния.
6. `evolution_agent.py` содержит дублирующий класс исполнения и собственную таблицу лотов; выяснить, используется ли он, до любых изменений.
7. `clear_redis.py` использует FLUSHALL; перед запуском добавить безопасный режим подтверждения.
8. В Git-дереве обнаружены `.venv/` и `__pycache__/`. Проверить .gitignore и удалить из отслеживания только генерируемые артефакты; локальные файлы пользователя не затрагивать.
9. Проверить, где реально должны жить `templates`, исторические CSV, RAG JSON, торговый журнал, логи и отчёты, чтобы не сломать старые установки.

## Этапы реализации и проверки

1. Завершить инвентаризацию всех Python-файлов, тестов, импортов, динамических импортов и файловых путей.
2. Добавить пакетную структуру и точки входа.
3. Перенести служебные утилиты первыми; проверить запуск из корня и из папки инструмента.
4. Перенести runtime-модули с совместимыми импортами; проверить отсутствие циклических импортов и дублирующих singleton/global state.
5. Перенести ресурсы и обновить пути.
6. Обновить тесты и добавить smoke tests:
   - импорт главного приложения без старта торгового цикла;
   - импорт Finam/Risk/Execution/AI/Storage;
   - запуск каждого безопасного служебного инструмента в режиме help/dry-run;
   - проверка нахождения шаблонов, конфигурации и данных;
   - pytest.
7. Подготовить отдельную инструкцию GitHub Desktop и локального запуска.

## Текущий статус

Создана ветка `refactor/project-structure` от актуальной `fix/bbo-sltp-top15-terminal`.
Фактические перемещения файлов пока не выполнялись: сначала завершается аудит, чтобы не повредить рабочий запуск и торговый контур.


## Дополнительные результаты аудита

- Текущий `.gitignore` уже содержит правила для `.env`, `venv/`, `.venv/`, `__pycache__/`, `trading_logs/`, `import_knowledge/` и `historical_ticks/`. При этом `.venv/`, `import_knowledge/` и `historical_ticks/` присутствуют в Git tree как отслеживаемые элементы: правило ignore не перестаёт отслеживать файлы, которые уже были добавлены в Git. Перед удалением из индекса нужно определить, какие данные должны оставаться в репозитории, и не удалять локальные копии.
- `requirements.txt` — единственный обнаруженный файл зависимостей; при переносе модулей менять набор зависимостей не требуется.
- Тесты импортируют корневые имена модулей (`candidate_recovery`, `risk_limits`). При перемещении нужны совместимые импорты или обновление тестов одновременно с переносом.
- `import_knowledge/inject_txt_reglement.py` содержит жёсткий путь `C:\\projects\\Piluitus\\import_knowledge` и пишет результат в относительный файл корня. Его нужно перевести на пути от `PROJECT_ROOT`.
- `historical_downloader.py` содержит собственный список инструментов, отличный от конфигурационного списка 88 акций. В рамках структурной задачи не синхронизировать и не менять этот список автоматически — это отдельное функциональное решение.
- `tg_bot.py` использует относительный путь `trading_logs` и ожидает старый шаблон имени журнала `piluitus_session_*.txt`, тогда как актуальная система логирования создаёт три файла `session_*.log`, `diagnostic_*.log`, `errors_log_*.log`. Это отдельное несоответствие поведения, не исправлять молча в рамках переносов.
- `market_logger.py` использует относительный каталог `live_market_logs`; после переноса должен быть закреплён путь от корня проекта.
- `rag_ai_distiller.py` жёстко задаёт PDF_DIR абсолютным Windows-путём и OUTPUT_FILE относительным путём. Перевести на PROJECT_ROOT и аргументы командной строки, сохранив значения по умолчанию.
- `clear_redis.py` выполняет `FLUSHALL` без пароля/DB-параметра из полного Settings и без подтверждения. Это опасный сценарий; безопасное подтверждение и точный scope очистки должны быть добавлены отдельным осознанным изменением, не менять очистку автоматически в рамках простого перемещения.


## Выполнено: первый этап перемещения служебных скриптов

Ветка: `refactor/project-structure`.

- Реализации `clear_redis.py` и `clean_rag.py` перенесены в `tools/maintenance/`.
- Реализации `historical_downloader.py` и `history_to_rag.py` перенесены в `tools/data/`.
- Реализации `rag_ai_distiller.py`, `merge_rules.py` и `inject_txt_reglement.py` перенесены в `tools/knowledge/`.
- В прежних местах оставлены совместимые launchers, чтобы старые команды запуска продолжали работать.
- Добавлен bootstrap корня проекта: утилиты могут импортировать корневые модули и запускаться независимо от текущей рабочей директории.
- Пути этих утилит к историческим CSV, PDF, TXT и JSON теперь вычисляются относительно корня проекта, без жёсткой привязки к `C:\\projects\\Piluitus`.
- `clear_redis.py` теперь по умолчанию ничего не очищает; для запуска требуется `--confirm`, используется `FLUSHDB` только выбранной базы, а не опасный `FLUSHALL`.

Примеры новых команд:
- `python tools/maintenance/clear_redis.py --help`
- `python tools/maintenance/clean_rag.py`
- `python tools/data/historical_downloader.py`
- `python tools/data/history_to_rag.py`
- `python tools/knowledge/rag_ai_distiller.py`
- `python tools/knowledge/merge_rules.py`
- `python tools/knowledge/inject_txt_reglement.py`

Корневые compatibility-launchers оставлены намеренно. Перенос торговых runtime-модулей пока не выполнен: он требует отдельного прохода по статическим и динамическим импортам, чтобы сохранить единый экземпляр глобальных объектов и не нарушить запуск `python main.py`.


## Выполнено: перенос основных модулей в пакеты

В корне оставлены совместимые import-shims. Они перенаправляют импорт на исходный объект модуля внутри пакета, чтобы старые импорты из `main.py`, тестов и динамических загрузчиков продолжали получать один и тот же экземпляр модуля.

Перенесены:
- `piluitus/exchange/`: `finam_client.py`.
- `piluitus/strategy/`: `screener.py`, `screener_funnel.py`, `minute_bar_patterns.py`, `signal_guard.py`, `candidate_recovery.py`.
- `piluitus/ai/`: `ai_engine.py`, `vllm_client.py`, `rag_prompt_utils.py`, `knowledge_parser.py`, `news_parser.py`, `dashboard_verdicts.py`.
- `piluitus/risk/`: `risk_manager.py`, `risk_limits.py`.
- `piluitus/execution/`: `execution_layer.py`.
- `piluitus/storage/`: `db_manager.py`.
- `piluitus/portfolio/`: `portfolio_metrics.py`, `pnl_exporter.py`.
- `piluitus/monitoring/`: `market_logger.py`.
- `interface/`: `tg_bot.py`.

`main.py` и `config.py` оставлены в корне как точки входа и единая конфигурация. `templates/` и данные пока не перемещались: пути к ним используются несколькими частями приложения, и сначала нужно завершить проверку всех потребителей. `evolution_agent.py` оставлен в корне до отдельной проверки его назначения и использования.

Это перенос с обратной совместимостью, а не одномоментная перепись импортов: старые абсолютные импорты продолжают работать через shims, новые модули доступны через пакетные пути.
