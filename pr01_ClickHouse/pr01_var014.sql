-- Задание 1. Создаём таблицу sales_var014
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


---Задание 2
--- 2.1 Общая выручка по категориям
SELECT
    category,
    round(SUM(quantity * unit_price * (1 - discount_pct)), 2) AS total_revenue
FROM sales_var014
GROUP BY category
ORDER BY total_revenue DESC;


---2.2 Топ-3 клиента по количеству покупок
SELECT
    customer_id,
    COUNT(*) AS purchase_count,
    SUM(quantity) AS total_quantity
FROM sales_var014
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;

---2.3 Средний чек по месяцам
SELECT
    toYYYYMM(sale_timestamp) AS month,
    round(AVG(quantity * unit_price), 2) AS avg_check
FROM sales_var014
GROUP BY month
ORDER BY month;

--- 2.4 Фильтрация по партиции
SELECT *
FROM sales_var014
WHERE sale_timestamp >= '2024-10-01' AND sale_timestamp < '2024-11-01';

---Задание 3.Создаём таблицу products_var014
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

---3.1 Вставляем 9 товаров (NNN % 10 + 5 = 14 % 10 + 5 = 4 + 5 = 9) с version = 1
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

---3.2 Для 3 товаров вставляем обновлённые записи с version = 2
INSERT INTO products_var014 VALUES
(140, 'Ноутбук Pro', 'Electronics', 'TechCorp', 899.99, 2.5, 0, now(), 2),   -- цена снижена, нет в наличии
(143, 'Кофемашина', 'Home', 'HomeGoods', 69.99, 3.0, 0, now(), 2),          -- цена снижена, нет в наличии
(145, 'Смартфон', 'Electronics', 'TechCorp', 649.99, 0.18, 1, now(), 2);     -- цена снижена

---3.3 Проверяем — видны обе версии
SELECT * FROM products_var014;

---3.4 Выполняем OPTIMIZE для слияния
OPTIMIZE TABLE products_var014 FINAL;

---3.5 Проверяем — осталась только версия 2
SELECT * FROM products_var014;

---3.6 Результаты запроса
SELECT * FROM products_var014 FINAL;


---Задание 4. SummingMergeTree — агрегация метрик. Создаём таблицу daily_metrics_var014
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

---4.1 Вставляем данные за 7 дней для 4 кампаний, по 2 канала каждая
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

---4.2 Вставляем повторные строки с теми же ключами (для проверки суммирования)
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

---4.3 Проверяем данные до оптимизации (должно быть много строк)
SELECT COUNT(*) AS total_rows FROM daily_metrics_var014;

---4.4 Выполняем OPTIMIZE для принудительного суммирования
OPTIMIZE TABLE daily_metrics_var014 FINAL;

---4.5 Проверяем, что данные просуммировались
-- Смотрим, сколько строк осталось (должно быть 7 дней × 4 кампании × 2 канала = 56 строк)
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

---4.6 CTR (Click-Through Rate) по каналам
SELECT
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    round(SUM(clicks) / SUM(impressions), 4) AS CTR
FROM daily_metrics_var014
GROUP BY channel;

---Задание 5. 
---5.1 Проверка партиций таблицы sales_var014
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

---(тк нет прав используем это)
SELECT 
    toYYYYMM(sale_timestamp) AS partition,
    COUNT(*) AS total_rows
FROM sales_var014
GROUP BY partition
ORDER BY partition;

---5.2 JOIN между sales_var014 и products_var014
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

---5.3 Типы данных всех трёх таблиц
DESCRIBE TABLE sales_var014;
DESCRIBE TABLE products_var014;
DESCRIBE TABLE daily_metrics_var014;

---5.4 Запрос с массивом (Array(String))
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

---5.5 Контрольная сумма (итоговая проверка)
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





