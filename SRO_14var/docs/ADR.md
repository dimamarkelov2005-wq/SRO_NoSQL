# Architecture Decision Record (ADR)

**Title:** Выбор Apache Cassandra для журнала событий веб-сервиса  
**Status:** Accepted  
**Date:** 2026-10-14  
**Variant:** 14 — Журнал событий веб-сервиса  

---

## Context

Необходимо хранить поток логов (events) от нескольких микросервисов с требованиями:

- высокая скорость записи (write-heavy);
- выборка «последние N событий сервиса»;
- фильтрация по уровню (ERROR / WARN / …);
- контроль размера партиций и защита от hot partition;
- горизонтальное масштабирование без ручного шардинга.

Рассматривались альтернативы: MongoDB, Redis, PostgreSQL, Elasticsearch.

## Decision Drivers

| Драйвер | Важность |
|---------|----------|
| Write throughput | Высокая |
| Bounded partition size | Высокая |
| Time-series access patterns | Высокая |
| Operational complexity | Средняя |
| Full-text search | Низкая (не требуется) |
| Strong consistency | Низкая (eventual достаточно) |

## Considered Alternatives

### 1. MongoDB (document store)
- **Плюсы:** гибкая схема, агрегации, индексы.
- **Минусы:** при росте объёма логов один collection становится горячей; TTL-индексы и capped collections не дают такого же предсказуемого контроля размера партиций, как composite partition key в Cassandra. Write amplification при частых обновлениях индексов.

### 2. Redis (in-memory)
- **Плюсы:** очень быстрые операции, Streams / Sorted Sets.
- **Минусы:** данные в основном в RAM → дорого для длительного хранения логов; персистентность AOF/RDB не предназначена для multi-TB event history; нет встроенного time-bucket partitioning.

### 3. PostgreSQL + TimescaleDB
- **Плюсы:** SQL, mature tooling, hypertables.
- **Минусы:** вертикальное масштабирование; при высокой write-нагрузке WAL и vacuum становятся узким местом; сложнее горизонтально шардировать.

### 4. Elasticsearch
- **Плюсы:** полнотекстовый поиск, агрегации.
- **Минусы:** избыточен для простой «последние N + фильтр по level»; дорого по ресурсам; не нужен full-text в данном проекте.

### 5. Apache Cassandra (выбрано)
- **Плюсы:**
  - Модель данных «query-first»: partition key = (service, day) → каждая партиция ограничена одним днём.
  - CLUSTERING ORDER BY event_time DESC → «последние события» — просто LIMIT без сортировки.
  - TimeWindowCompactionStrategy идеально подходит для time-series.
  - Линейное горизонтальное масштабирование.
  - TTL на уровне таблицы.
- **Минусы:**
  - Нет JOIN / произвольных агрегаций (решается денормализацией — таблица events_by_level).
  - Кросс-дневные запросы выполняются на стороне приложения (несколько partition reads).

## Decision

**Принято:** использовать Apache Cassandra 4.x / 5.x с Python-драйвером.

### Модель данных

```
PRIMARY KEY ((service_name, day), event_time, event_id)
CLUSTERING ORDER BY (event_time DESC, event_id DESC)
```

- **Daily bucketing** ограничивает размер партиции.
- Для сервисов с очень высоким RPS дополнительно ведётся таблица с **hourly bucket** (`events_by_service_hour`), что демонстрирует технику борьбы с hot partition.
- Таблица `events_by_level` — денормализация под Access Pattern «все ERROR сервиса за день».

### Обоснование против hot partition

Без bucket’а partition key = service_name → все события одного популярного сервиса попадают в одну партицию, которая растёт бесконечно (классический hot partition).  
Составной ключ (service_name, day) распределяет данные по дням; при необходимости можно добавить hour или random salt.

## Consequences

**Positive**
- Предсказуемая latency чтения «последних N».
- Контролируемый размер партиций.
- Простое масштабирование (добавление нод).

**Negative / Trade-offs**
- Приложение само объединяет результаты нескольких day-buckets.
- Изменение schema (добавление нового Access Pattern) обычно требует новой таблицы (denormalization).
- Операционные навыки Cassandra (repair, compaction) обязательны в production.

## References

- DataStax: Time Series Data Modeling
- The Last Pickle: Bucketing for massive scale
- Cassandra documentation: TimeWindowCompactionStrategy
