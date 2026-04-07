# Практическое занятие №1. 

**Тема:** Основы ClickHouse: установка, типы данных, движки таблиц. 

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



**Скриншот плана после индексации:**


**Сравнение:**
Время выполнения уменьшилось с с 179.051 мс до 88.852 мс.

---


| Задание | Без индекса | С индексом | Ускорение |
|---------|-------------|------------|-----------|
| **Задание 2** (поиск по дате) | 6.682 ms | 0.436 ms | **↓ в 15.3 раза** |
| **Задание 3** (JOIN + точное совпадение) | 179.051 ms | 88.852 ms | **↓ в 2.01 раза** |


##  Выводы

* B-Tree индекс на поле date_opened ускорил запрос в 15.3 раза
* B-Tree индекс на поле email_subject ускорил сложный JOIN-запрос в 2.01 раза
* Количество прочитанных страниц памяти уменьшилось в 4.5-8 раз
* Планировщик изменил стратегию с Seq Scan на Bitmap Index Scan, что позволило читать только нужные данные

