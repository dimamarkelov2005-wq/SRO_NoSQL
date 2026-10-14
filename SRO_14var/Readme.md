# Вариант 14. Журнал событий веб-сервиса (Cassandra)

Прототип NoSQL-подсистемы журналирования событий микросервисов на **Apache Cassandra**.

**Курс:** 3, «Информационные системы»

---

## Краткое описание

Система принимает события (логи) от нескольких веб-сервисов, хранит их с **time-bucketing** (день / час) и позволяет эффективно:

- получать последние N событий сервиса;
- фильтровать по уровню (ERROR, WARN, …);
- выполнять диапазонные запросы по времени (в т.ч. кросс-дневные);
- контролировать размер партиций и демонстрировать борьбу с **hot partition**.

## Стек

| Компонент        | Технология              |
|------------------|-------------------------|
| СУБД             | Apache Cassandra 4.x/5.x|
| Язык             | Python 3.10+            |
| Драйвер          | cassandra-driver        |
| Тесты            | pytest                  |
| Контейнер (опц.) | Docker                  |

## Структура репозитория

```
README.md
.env.example
requirements.txt
src/
  config.py
  db.py
  models.py
  repository.py
  schema.py
data/
  sample_events.json
scripts/
  seed.py              # генерация ≥ 30 (фактически 60) синтетических событий
  demo_queries.py      # ≥ 8 демонстрационных запросов (2 нетривиальных)
tests/
  test_repository.py   # ≥ 3 тестовых сценария
docs/
  ADR.md               # Architecture Decision Record
  architecture.md      # схема и описание
  architecture.png     # (сгенерировать / нарисовать)
  report.pdf           # отчёт 8–12 стр.
  presentation.pdf     # презентация 8–10 слайдов
```

## Требования (сводка)

### Функциональные (≥ 6)
1. Регистрация сервисов.
2. Запись события (CREATE).
3. Получение последних N событий сервиса за день.
4. Фильтрация событий по уровню.
5. Диапазонный запрос по времени (в т.ч. несколько day-buckets).
6. Удаление события (DELETE).
7. Подсчёт событий в партиции.
8. Запись в hourly-bucket (анти-hot-partition).

### Нефункциональные (≥ 3)
1. Размер партиции ограничен одним днём (или часом).
2. Latency чтения «последних N» — O(limit) за счёт clustering order.
3. TTL 30 дней (7 дней для hourly).
4. Горизонтальное масштабирование Cassandra.

### Access Patterns (≥ 8)

| # | Pattern | Приоритет | Ожидаемый результат |
|---|---------|-----------|---------------------|
| AP1 | Последние N событий сервиса за день | Высокий | ≤ N строк, newest first |
| AP2 | События уровня L сервиса за день | Высокий | Список ERROR/WARN/… |
| AP3 | События сервиса в интервале [t1, t2] (кросс-день) | Средний | Объединённый список |
| AP4 | Последние N из hourly-bucket | Средний | Демонстрация anti-hot |
| AP5 | COUNT событий в day-partition | Низкий | Число |
| AP6 | Список зарегистрированных сервисов | Низкий | Метаданные |
| AP7 | Удаление конкретного события | Средний | Tombstone → отсутствие в SELECT |
| AP8 | Upsert (изменение message) | Низкий | Новое значение при повторном чтении |

## Быстрый старт

### 1. Cassandra (Docker)

```bash
docker run --name cassandra-event-log -p 9042:9042 -d cassandra:4.1
# дождитесь готовности (~30–60 с)
docker exec -it cassandra-event-log cqlsh -e "DESCRIBE KEYSPACES;"
```

### 2. Python-окружение

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

### 3. Инициализация схемы + seed

```bash
python -m scripts.seed
```

### 4. Демонстрационные запросы

```bash
python -m scripts.demo_queries
```

### 5. Тесты

```bash
pytest tests/ -v
```

## Модель данных (ключевой фрагмент)

```cql
CREATE TABLE events_by_service_day (
    service_name text,
    day          text,          -- 'YYYY-MM-DD'
    event_time   timestamp,
    event_id     timeuuid,
    level        text,
    message      text,
    ...
    PRIMARY KEY ((service_name, day), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy', ...};
```

## Документация

- [ADR — обоснование выбора Cassandra](docs/ADR.md)
- [Архитектура](docs/architecture.md)
- Отчёт: `docs/report.pdf`
- Презентация: `docs/presentation.pdf`

## Ограничения прототипа

- Один узел Cassandra (SimpleStrategy, RF=1) — достаточно для демо.
- Кросс-дневные запросы выполняются последовательно (можно заменить на `execute_concurrent`).
- Нет аутентификации/авторизации на уровне приложения.
- Синтетические данные, без реальных PII.
