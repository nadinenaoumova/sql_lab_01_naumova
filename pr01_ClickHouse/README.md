# Практическое занятие №1. Основы ClickHouse: установка, типы данных, движки таблиц. 

**Вариант:** 14

---

##  Цель работы

Получить практические навыки работы с колоночной СУБД ClickHouse: подключиться к облачному серверу, освоить создание баз данных и таблиц с правильным выбором типов данных и движков семейства MergeTree.

---

##  Задание 1. Создание базы данных и таблицы продаж

**SQL-запрос:**

```sql
CREATE TABLE sales_var014 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       LowCardinality(String),
    customer_id    UInt64,
    region         LowCardinality(String),
    quantity       UInt16,
    unit_price     Decimal64(2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     IPv4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);

-- Вставляем данные за 3 месяца (Октябрь, Ноябрь, Декабрь 2024)
-- Всего 120 строк

INSERT INTO sales_var014 
(sale_id, sale_timestamp, product_id, category, customer_id, region, quantity, unit_price, discount_pct, is_online, ip_address)
SELECT
    number + 14001 AS sale_id,
    toDateTime64('2024-10-01 00:00:00', 3) + INTERVAL (number % 90) DAY + INTERVAL (number % 24) HOUR,
    140 + (number % 20) AS product_id,
    CASE (number % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Books'
        WHEN 3 THEN 'Home'
        ELSE 'Sports'
    END AS category,
    1400 + (number % 100) AS customer_id,
    CASE (number % 4)
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END AS region,
    (number % 19) + 1 AS quantity,
    24.00 + (number % 487) AS unit_price,
    (number % 100) / 100.0 AS discount_pct,
    (number % 2) AS is_online,
    IPv4StringToNum(concat(toString(number % 256), '.', toString((number + 64) % 256), '.', toString((number + 128) % 256), '.', toString(number % 256))) AS ip_address
FROM numbers(120);
```

**Требования к данным по вариантам:**

- sale_id начинается с 14001
- customer_id лежит в диапазоне [1400 .. 1499]
- product_id лежит в диапазоне [140 .. 159]
- unit_price — от 24 до 524.00.
- quantity — от 1 до 19

---

##  Задание 2. Аналитические запросы

**Условие:**
Напишите и выполните 4 запроса к таблице sales_var014: 

**2.1 Общая выручка по категориям**

```sql
SELECT
    category,
    round(SUM(quantity * unit_price * (1 - discount_pct)), 2) AS total_revenue
FROM sales_var014
GROUP BY category
ORDER BY total_revenue DESC;
```
**Скриншот выполнения:**
<img width="503" height="239" alt="image" src="https://github.com/user-attachments/assets/e71c808c-2618-4d53-b7e6-b999771680ba" />

**2.2 Топ-3 клиента по количеству покупок**

```sql
SELECT
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var014
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;
```

**Скриншот выполнения:**
<img width="773" height="158" alt="image" src="https://github.com/user-attachments/assets/9b0dcc24-3e12-4474-b879-fde28907185b" />


**2.3 Средний чек по месяцам**

```sql
SELECT
    toYYYYMM(sale_timestamp) AS month,
    round(AVG(quantity * unit_price), 2) AS avg_check
FROM sales_var014
GROUP BY month
ORDER BY month;
```

**Скриншот выполнения:**
<img width="458" height="167" alt="image" src="https://github.com/user-attachments/assets/5e3a46d7-f3f9-40d6-a43e-daff8c5b83e6" />


**2.4 Фильтрация по партиции**

```sql
SELECT *
FROM sales_var014
WHERE sale_timestamp >= '2024-10-01' AND sale_timestamp < '2024-11-01';
```
**Скриншот выполнения:**
<img width="974" height="388" alt="image" src="https://github.com/user-attachments/assets/9da99c8f-2937-4082-9317-1d2b5e3f0e03" />


---

##  Задание 3. ReplacingMergeTree — справочник товаров

**Условие:**
Создайте таблицу products_var014. 
1. Вставьте 9 товаров с version = 1.
2. Для 3 товаров вставьте обновлённые записи с version = 2 (измените base_price и is_available).
3. Выполните SELECT * FROM products_var014 — убедитесь, что видны обе версии.
4. Выполните OPTIMIZE TABLE products_varNNN FINAL.
5. Повторите SELECT — убедитесь, что осталась только версия 2.
6. Покажите результаты запроса SELECT * FROM products_varNNN FINAL (альтернатива OPTIMIZE).

**3.1**

```sql
---Создаём таблицу products_var014
CREATE TABLE products_var014 (
    product_id    UInt32,
    product_name  String,
    category      LowCardinality(String),
    supplier      String,
    base_price    Decimal64(2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id);

---3.1 Вставляем 9 товаров с version = 1
INSERT INTO products_var014 VALUES
(140, 'Ноутбук Pro', 'Electronics', 'TechCorp', 999.99, 2.5, 1, now(), 1),
(141, 'Футболка', 'Clothing', 'FashionInc', 29.99, 0.2, 1, now(), 1),
(142, 'Книга Python', 'Books', 'PubHouse', 49.99, 0.5, 1, now(), 1),
(143, 'Кофемашина', 'Home', 'HomeGoods', 79.99, 3.0, 1, now(), 1),
(144, 'Мяч футбольный', 'Sports', 'SportCo', 24.99, 0.45, 1, now(), 1),
(145, 'Смартфон', 'Electronics', 'TechCorp', 699.99, 0.18, 1, now(), 1),
(146, 'Джинсы', 'Clothing', 'FashionInc', 59.99, 0.7, 1, now(), 1),
(147, 'Тетрадь', 'Books', 'PubHouse', 12.99, 0.3, 1, now(), 1),
(148, 'Лампа', 'Home', 'HomeGoods', 34.99, 1.2, 1, now(), 1);
```

**3.2**
```sql
--- Для 3 товаров вставляем обновлённые записи с version = 2
INSERT INTO products_var014 VALUES
(140, 'Ноутбук Pro', 'Electronics', 'TechCorp', 899.99, 2.5, 0, now(), 2),   -- цена снижена, нет в наличии
(143, 'Кофемашина', 'Home', 'HomeGoods', 69.99, 3.0, 0, now(), 2),          -- цена снижена, нет в наличии
(145, 'Смартфон', 'Electronics', 'TechCorp', 649.99, 0.18, 1, now(), 2);     -- цена снижена
```
**3.3**
```sql
SELECT * FROM products_var014;
```

**3.4**
```sql
OPTIMIZE TABLE products_var014 FINAL;
```

**3.5**
```sql
---Проверяем — осталась только версия 2
SELECT * FROM products_var014;
```

**3.6**
```sql
SELECT * FROM products_var014 FINAL;
```
**Скриншот выполнения:**
<img width="876" height="400" alt="image" src="https://github.com/user-attachments/assets/25c394a9-806c-4d2f-940e-9af90187434b" />


##  Задание 4. ReplacingMergeTree — справочник товаров

**Условие:**
Создайте таблицу daily_metrics_var014 в базе db_var014. 
1. Вставьте данные за 7 дней для 4 кампаний по 2 канала каждая. campaign_id начинается с 141.
2. Вставьте повторные строки с теми же ключами (metric_date, campaign_id, channel), но с другими значениями метрик.
3. Выполните OPTIMIZE TABLE daily_metrics_varN014 FINAL.
4. Убедитесь, что числовые столбцы (impressions, clicks, conversions, spend_cents) просуммировались для строк с одинаковыми ключами.
5. Напишите запрос: суммарные clicks / impressions (CTR) по каналам.

**4.1**
```sql
CREATE TABLE daily_metrics_var014 (
    metric_date    Date,
    campaign_id    UInt32,
    channel        LowCardinality(String),
    impressions    UInt64,
    clicks         UInt64,
    conversions    UInt32,
    spend_cents    UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, campaign_id, channel);

---Вставляем данные за 7 дней для 4 кампаний, по 2 канала каждая
INSERT INTO daily_metrics_var014
SELECT
    toDate('2024-10-01') + INTERVAL (number % 7) DAY AS metric_date,
    141 + (number % 4) AS campaign_id,
    CASE (number % 2) WHEN 0 THEN 'Email' ELSE 'Social' END AS channel,
    1000 + (number % 5000) AS impressions,
    50 + (number % 300) AS clicks,
    1 + (number % 20) AS conversions,
    5000 + (number % 10000) AS spend_cents
FROM numbers(56);
```

**4.2**
```sql
---Вставляем повторные строки с теми же ключами (для проверки суммирования)
INSERT INTO daily_metrics_var014
SELECT
    toDate('2024-10-01') + INTERVAL (number % 7) DAY AS metric_date,
    141 + (number % 4) AS campaign_id,
    CASE (number % 2) WHEN 0 THEN 'Email' ELSE 'Social' END AS channel,
    500 + (number % 1000) AS impressions,
    10 + (number % 100) AS clicks,
    1 + (number % 10) AS conversions,
    1000 + (number % 5000) AS spend_cents
FROM numbers(28);
```

**4.3**
```sql
---Проверяем данные до оптимизации (должно быть много строк)
SELECT COUNT(*) AS total_rows FROM daily_metrics_var014;
```

**4.4**
```sql
---Выполняем OPTIMIZE для принудительного суммирования
OPTIMIZE TABLE daily_metrics_var014 FINAL;
```

**4.5**
```sql
---Проверяем, что данные просуммировались. Смотрим, сколько строк осталось (должно быть 7 дней × 4 кампании × 2 канала = 56 строк)
SELECT 
    metric_date,
    campaign_id,
    channel,
    impressions,
    clicks,
    conversions,
    spend_cents
FROM daily_metrics_var014
ORDER BY metric_date, campaign_id, channel;
```

**4.6**
```sql
---CTR (Click-Through Rate) по каналам
SELECT
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    round(SUM(clicks) / SUM(impressions), 4) AS CTR
FROM daily_metrics_var014
GROUP BY channel;
```
**Скриншот выполнения:**
<img width="765" height="471" alt="image" src="https://github.com/user-attachments/assets/e710f911-cf0a-4392-8742-2bbb2c524a69" />


##  Задание 5. Комплексный анализ и самопроверка

**Условие:**
Выполните следующие запросы и зафиксируйте результаты:

**5.1 Проверка партиций таблицы sales_var014:**

```sql
SELECT
    partition,
    count() AS parts,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'db_14'
  AND table = 'sales_var014'
  AND active
GROUP BY partition
ORDER BY partition;
```
**Скриншот выполнения:**
<img width="441" height="153" alt="image" src="https://github.com/user-attachments/assets/e419d569-df2e-46ef-ab5c-06afeb9d12fe" />


**5.2 JOIN между таблицами — соедините sales_varNNN с products_varNNN по product_id и выведите топ-5 товаров по суммарной выручке:**
```sql
SELECT
    p.product_name,
    p.category,
    round(sum(s.quantity * s.unit_price * (1 - s.discount_pct)), 2) AS revenue
FROM sales_var014 AS s
INNER JOIN products_var014 AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;
```
**Скриншот выполнения:**
<img width="655" height="228" alt="image" src="https://github.com/user-attachments/assets/def4f5c7-673b-43c5-b0e4-7211f42a0557" />


**5.3 Типы данных — выведите структуру всех трёх созданных таблиц:**

```sql
DESCRIBE TABLE sales_var014;
DESCRIBE TABLE products_var014;
DESCRIBE TABLE daily_metrics_var014;
```

**Скриншот выполнения:**
<img width="470" height="443" alt="image" src="https://github.com/user-attachments/assets/34f44b79-4b03-45dd-894e-92293c13669b" />
<img width="403" height="361" alt="image" src="https://github.com/user-attachments/assets/2deaff9d-6b1c-4112-bfbc-30b44dd7e210" />
<img width="475" height="302" alt="image" src="https://github.com/user-attachments/assets/a2f9e27f-c2f7-4898-abf8-ad809ac4452d" />


**5.4 Запрос с массивом — создайте временную таблицу tags_var014 с колонкой Array(String) и выполните запрос с arrayJoin:**

```sql
-- Создаём временную таблицу tags_var014
CREATE TABLE tags_var014 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
) ENGINE = MergeTree()
ORDER BY item_id;

-- Вставляем данные
INSERT INTO tags_var014 VALUES
(1, 'Item A', ['sale', 'popular', 'new']),
(2, 'Item B', ['premium', 'limited']),
(3, 'Item C', ['sale', 'clearance']);

-- Запрос с arrayJoin
SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM tags_var014
GROUP BY tag
ORDER BY items_count DESC;
```
**Скриншот выполнения:**
<img width="370" height="264" alt="image" src="https://github.com/user-attachments/assets/7f448de4-e811-40d9-94ff-5b4be8c05274" />


**5.5 Контрольная сумма — итоговая проверка:**

```sql
SELECT
    'sales' AS tbl, 
    count() AS rows, 
    sum(quantity) AS check_sum 
FROM db_14.sales_var014

UNION ALL

SELECT
    'products', 
    count(), 
    sum(toUInt64(product_id)) 
FROM db_14.products_var014 FINAL

UNION ALL

SELECT
    'metrics', 
    count(), 
    sum(clicks) 
FROM daily_metrics_var014;
```

**Скриншот выполнения:**
<img width="558" height="159" alt="image" src="https://github.com/user-attachments/assets/26b0545d-5724-41c7-bfc2-a95cbaec2435" />


**1. Почему LowCardinality(String) эффективнее обычного String для столбца category?**
LowCardinality хранит уникальные значения отдельно и заменяет их на числовые идентификаторы, что уменьшает объём данных и ускоряет операции. Это особенно эффективно для столбцов с повторяющимися значениями, таких как категории.

**2. В чём разница между ORDER BY и PRIMARY KEY в ClickHouse?**
ORDER BY определяет физический порядок хранения данных и используется для сортировки и сжатия. PRIMARY KEY — это выражение для быстрого поиска, которое логически связано с ORDER BY и обычно является его префиксом.

**3. Когда следует использовать ReplacingMergeTree вместо MergeTree?**
ReplacingMergeTree применяют, когда нужно устранять дубликаты строк или хранить только последнюю версию записи. Он полезен при обновлениях данных без явных операций UPDATE.

**4. Почему SummingMergeTree не заменяет GROUP BY в аналитических запросах?**
SummingMergeTree агрегирует данные только во время слияния частей и не гарантирует актуальные результаты в каждый момент времени. Поэтому для точных расчётов всё равно нужен GROUP BY.

**5. Что произойдёт, если не выполнить OPTIMIZE TABLE FINAL для ReplacingMergeTree?**
Дубликаты могут остаться в таблице, так как слияние частей происходит не сразу. В результате запросы могут возвращать устаревшие или повторяющиеся данные.









