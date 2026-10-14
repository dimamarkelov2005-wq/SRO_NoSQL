# Архитектурная схема — Журнал событий веб-сервиса (Variant 14)

## Высокоуровневая архитектура

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  auth-service   │     │ payment-service │     │  api-gateway    │
│  order-service  │     │ notification-…  │     │  (high volume)  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │  emit Event
                                 ▼
                    ┌────────────────────────┐
                    │   Event Log Producer   │
                    │   (Python repository)  │
                    └────────────┬───────────┘
                                 │  CQL INSERT
                                 ▼
              ┌──────────────────────────────────────┐
              │         Apache Cassandra             │
              │                                      │
              │  events_by_service_day               │
              │    PK ((service, day), time, id)     │
              │                                      │
              │  events_by_service_hour  (hot mit.)  │
              │    PK ((service, hour), time, id)    │
              │                                      │
              │  events_by_level                     │
              │    PK ((service, day, level), …)     │
              │                                      │
              │  services  (metadata)                │
              └──────────────────────────────────────┘
                                 ▲
                                 │  CQL SELECT
                    ┌────────────┴───────────┐
                    │   Query / Demo layer   │
                    │   (demo_queries.py)    │
                    └────────────────────────┘
```

## Ключевые решения по модели данных

| Таблица                    | Partition key              | Clustering              | Назначение                          |
|---------------------------|----------------------------|-------------------------|-------------------------------------|
| events_by_service_day     | (service_name, day)        | event_time DESC, id     | Основной журнал, daily bucket       |
| events_by_service_hour    | (service_name, hour_bucket)| event_time DESC, id     | Анти-hot-partition для high-RPS     |
| events_by_level           | (service_name, day, level) | event_time DESC, id     | Быстрый фильтр по уровню            |
| services                  | service_name               | —                       | Реестр сервисов                     |

## Поток записи

1. Producer формирует объект `Event` (timeuuid, timestamp, level, message…).
2. Вычисляются bucket’ы: `day = YYYY-MM-DD`, `hour = YYYY-MM-DD-HH`.
3. INSERT в `events_by_service_day` + `events_by_level`.
4. Для high-volume сервисов — дополнительный INSERT в `events_by_service_hour`.

## Поток чтения «последние N»

```
SELECT … FROM events_by_service_day
WHERE service_name = ? AND day = ?
LIMIT N;
```
Благодаря `CLUSTERING ORDER BY (event_time DESC)` сортировка не нужна.

## Кросс-дневной диапазон (non-trivial)

Приложение генерирует список day-buckets в интервале `[start, end]` и выполняет
параллельные (или последовательные) запросы, после чего сливает результаты.

## Защита от hot partition

- Daily (и при необходимости hourly) bucket ограничивает размер каждой партиции.
- TimeWindowCompactionStrategy согласован с размером bucket’а.
- TTL 30 дней (7 дней для hourly) автоматически удаляет старые данные без tombstone-шторма при правильном gc_grace.
