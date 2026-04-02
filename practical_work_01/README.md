# 📊 Практическая работа №1 — Геопространственный анализ данных. Аналитика с использованием сложных типов данных.

## 📌 Блок А. Анализ времени и дат

### 1. Дни недели продаж

**Задача:** Определите, в какой день недели (понедельник, вторник и т.д.) совершается наибольшее количество продаж (sales). Выведите день недели и количество транзакций.
```sql
SELECT 
    TO_CHAR(sales_transaction_date, 'Day') AS day_of_week,
    COUNT(*) AS transaction_count
FROM sales
GROUP BY 
    TO_CHAR(sales_transaction_date, 'Day'),
    EXTRACT(DOW FROM sales_transaction_date)
ORDER BY transaction_count DESC
LIMIT 1;
```

**Скриншот результата:**
<img width="552" height="133" alt="image" src="https://github.com/user-attachments/assets/41df607e-bf2f-45b0-949c-b8bec5833983" />


**Результат:**

* Максимальное количество транзакций: **5456**
* День недели: **вторник**

**Вывод:**
Во вторник наблюдается наибольшая покупательская активность.

---

### 2. Скорость реакции клиентов

**Задача:** Рассчитайте среднее время (интервал), которое проходит между регистрацией клиента (date_added в customers) и его первой покупкой (sales_transaction_date в sales).

```sql
WITH first_purchase AS (
    SELECT 
        customer_id,
        MIN(sales_transaction_date) AS first_buy_date
    FROM sales
    GROUP BY customer_id
)
SELECT 
    AVG(first_buy_date - date_added) AS avg_days_to_purchase
FROM customers c
INNER JOIN first_purchase fp ON c.customer_id = fp.customer_id;
```

**Скриншот результата:**
<img width="396" height="98" alt="image" src="https://github.com/user-attachments/assets/3a08305b-509b-439b-b0eb-1db7fb03e2e6" />


**Результат:**

* Среднее время: **433 дня 11 часов 53 минуты**

**Вывод:**
Клиенты в среднем совершают первую покупку спустя **~1 год и 2 месяца** после регистрации.

---

### 3. Сезонность продуктов

**Задача:** Для каждого типа продукта (product_type) определите месяц, в котором он продается лучше всего (максимальная сумма продаж).

```sql
WITH monthly_sales AS (
    SELECT 
        s.product_id,
        p.model AS product_model,
        EXTRACT(YEAR FROM s.sales_transaction_date) AS year,
        EXTRACT(MONTH FROM s.sales_transaction_date) AS month,
        SUM(s.sales_amount) AS total_sales
    FROM sales s
    LEFT JOIN products p ON s.product_id = p.product_id
    GROUP BY s.product_id, p.model, year, month
),
ranked_sales AS (
    SELECT 
        product_id,
        product_model,
        year,
        month,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY total_sales DESC) AS rank
    FROM monthly_sales
)
SELECT 
    product_id,
    product_model,
    year,
    month,
    TO_CHAR(TO_DATE(month::text, 'MM'), 'Month') AS month_name,
    ROUND(CAST(total_sales AS numeric), 2) AS max_sales_amount
FROM ranked_sales
WHERE rank = 1
ORDER BY product_id;
```

**Скриншот результата:**
<img width="974" height="506" alt="image" src="https://github.com/user-attachments/assets/cc7574ce-6dd5-4083-a706-b2df1557e93a" />


**Результат:**

* Пик продаж: май–июль
* Лидеры:

  * *Model Chi* — **4 335 500 (июнь 2016)**
  * *Model Sigma* — **4 002 050 (сентябрь 2016)**

**Вывод:**
Наблюдается выраженная сезонность с пиком в летние месяцы.

---

## 🌍 Блок Б. Геопространственный анализ

### 1. Ближайший дилер

**Задача:** Для каждого клиента из города 'New York City' найдите ближайший дилерский центр (dealerships) и расстояние до него.

```sql
SELECT DISTINCT ON (c.customer_id)
    c.customer_id,
    d.dealership_id,
    d.city AS dealer_city,
    ROUND(CAST((point(c.longitude, c.latitude) <@> point(d.longitude, d.latitude)) AS numeric), 2) AS distance_miles
FROM customers c
CROSS JOIN (SELECT * FROM dealerships LIMIT 500) d
WHERE c.city = 'New York City'
ORDER BY c.customer_id, distance_miles;
```

**Скриншот результата:**
<img width="692" height="498" alt="image" src="https://github.com/user-attachments/assets/07d0f4c0-739e-42db-b190-410cef8181c9" />


**Результат:**

* Проанализировано: **731 клиент**
* Для каждого найден ближайший дилер

---

### 2. Покрытие дилеров

**Задача:** Найдите дилерский центр, у которого наибольшее количество клиентов в радиусе 100 миль.

```sql
WITH dealer_coverage AS (
    SELECT 
        d.dealership_id,
        d.city AS dealer_city,
        COUNT(DISTINCT c.customer_id) AS customers_within_100_miles
    FROM dealerships d
    CROSS JOIN customers c
    WHERE (point(d.longitude, d.latitude) <@> point(c.longitude, c.latitude)) <= 100
    GROUP BY d.dealership_id, d.city
)
SELECT 
    dealership_id,
    dealer_city,
    customers_within_100_miles
FROM dealer_coverage
ORDER BY customers_within_100_miles DESC
LIMIT 1;
```
**Скриншот результата:**
<img width="648" height="100" alt="image" src="https://github.com/user-attachments/assets/3c6b8b7c-e13a-4f00-b31f-49288d07297e" />


**Результат:**

* Лидер по охвату: дилер в **Москве**

---

### 3. География модели (центроид)
**Задача:** Для модели 'Model Chi' найдите среднюю широту и долготу покупателей (центроид продаж).

```sql
SELECT 
    p.model AS product_model,
    AVG(c.latitude) AS avg_latitude,
    AVG(c.longitude) AS avg_longitude,
    ll_to_earth(AVG(c.latitude), AVG(c.longitude)) AS centroid_earth
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN customers c ON s.customer_id = c.customer_id
WHERE p.model = 'Model Chi'
GROUP BY p.model;
```

**Скриншот результата:** 
<img width="973" height="74" alt="image" src="https://github.com/user-attachments/assets/8724be92-26ba-44f3-9e4d-6e5abee080ec" />

**Результат:**

* Широта: **36.86**
* Долгота: **-93.49**

**Вывод:**
Определён географический центр продаж модели.

---

## 🧩 Блок В. Сложные типы (JSON и массивы)

### 1. История покупок в JSON
**Задача:** Создайте представление или запрос, который формирует JSON-объект для каждого клиента: { "id": 1, "name": "Ivan", "products": ["Car", "Scooter"] }, используя агрегацию массивов.

```sql
SELECT 
    c.customer_id AS id,
    CONCAT(c.first_name, ' ', c.last_name) AS name,
    JSONB_BUILD_OBJECT(
        'id', c.customer_id,
        'name', CONCAT(c.first_name, ' ', c.last_name),
        'products', COALESCE(
            (SELECT ARRAY_AGG(DISTINCT p.model) 
             FROM sales s 
             JOIN products p ON s.product_id = p.product_id 
             WHERE s.customer_id = c.customer_id), 
            ARRAY[]::text[]
        )
    ) AS customer_json
FROM customers c
ORDER BY c.customer_id;
```
**Скриншот результата:**
<img width="974" height="561" alt="image" src="https://github.com/user-attachments/assets/a13653a6-34bb-4a21-8117-98ca79d03ebe" />


**Вывод:**
Сформированы JSON-объекты с историей покупок клиентов.

---

### 2. Агрегация email по штатам
**Задача:** Сгруппируйте данные по штатам (state) и сформируйте массив email-адресов всех дилеров в этом штате.

```sql
SELECT 
    d.state,
    ARRAY_AGG(DISTINCT c.email) AS dealer_emails,
    COUNT(DISTINCT c.email) AS emails_count,
    COUNT(DISTINCT d.dealership_id) AS dealers_count
FROM dealerships d
LEFT JOIN contacts c ON d.dealership_id = c.company_id
WHERE d.state IS NOT NULL
  AND c.email IS NOT NULL
GROUP BY d.state
ORDER BY d.state;
```
**Скриншот результата:**
<img width="1001" height="451" alt="image" src="https://github.com/user-attachments/assets/17f17693-6e76-4608-965f-7a87e3c8aa6f" />

**Результат:**
По результатам агрегации данных по штатам сформированы массивы email-адресов контактных лиц дилерских центров. Наибольшее количество уникальных email-адресов (по 4) зафиксировано в штатах Florida и Texas, что соответствует трём дилерским центрам в каждом из этих штатов.

---

### 3. Тэгирование клиентов (VIP)
**Задача:** Добавьте к таблице customers текстовое поле-массив tags. Напишите запрос, который добавляет тег 'VIP' всем клиентам, совершившим покупки на сумму более 50000.

```sql
--- Шаг 1. Добавляем колонку tags
ALTER TABLE customers 
ADD COLUMN IF NOT EXISTS tags TEXT[];
--- Шаг 2. Добавляем тег 'VIP' клиентам с суммой покупок > 50000
UPDATE customers c
SET tags = ARRAY_APPEND(
    COALESCE(c.tags, ARRAY[]::TEXT[]), 
    'VIP'
)
WHERE c.customer_id IN (
    SELECT s.customer_id
    FROM sales s
    GROUP BY s.customer_id
    HAVING SUM(s.sales_amount) > 50000
);
--- Шаг 3. Проверяем результат
-- Сколько клиентов получили VIP
SELECT COUNT(*) AS vip_count
FROM customers
WHERE 'VIP' = ANY(tags);
-- Показываем VIP-клиентов
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS name,
    c.tags,
    ROUND(CAST(SUM(s.sales_amount) AS numeric), 2) AS total_spent
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.tags
HAVING 'VIP' = ANY(c.tags)
ORDER BY total_spent DESC;
```

**Скриншот результата:**
<img width="192" height="100" alt="image" src="https://github.com/user-attachments/assets/88cb1a2a-a635-4352-a8dd-18a5ba01d23a" />


**Результат:**

* VIP-клиенты: **2565**
* Максимальные траты: **231 414.97**

---

## 📝 Блок Г. Текстовая аналитика

### 1. Частотный словарь
**Задача:** Составьте топ-10 самых часто встречающихся слов в таблице customer_survey (столбец feedback), исключив слова короче 3 символов.

```sql
WITH all_words AS (
    SELECT 
        LOWER(REGEXP_REPLACE(feedback, '[^a-zA-Zа-яА-Я\s]', '', 'g')) AS cleaned_feedback
    FROM customer_survey
),
words_split AS (
    SELECT 
        UNNEST(STRING_TO_ARRAY(cleaned_feedback, ' ')) AS word
    FROM all_words
)
SELECT 
    word,
    COUNT(*) AS frequency
FROM words_split
WHERE LENGTH(word) >= 3
GROUP BY word
ORDER BY frequency DESC
LIMIT 10;
```
**Скриншот результата:**
<img width="330" height="453" alt="image" src="https://github.com/user-attachments/assets/e9715195-8682-48c3-8704-e59c82370f42" />


**Результат:**

* scooter — 13
* great — 11
* really — 8

---

### 2. Поиск негативных отзывов
**Задача:** Найдите все отзывы, содержащие слова с корнем 'bad', 'fail', 'poor' (используйте to_tsvector и plainto_tsquery или ILIKE).

```sql
SELECT 
    rating,
    feedback
FROM customer_survey
WHERE feedback ILIKE '%bad%' 
   OR feedback ILIKE '%fail%' 
   OR feedback ILIKE '%poor%' 
   OR feedback ILIKE '%terrible%' 
   OR feedback ILIKE '%awful%' 
   OR feedback ILIKE '%worst%' 
   OR feedback ILIKE '%issue%' 
   OR feedback ILIKE '%problem%'
ORDER BY rating;
```

**Скриншот результата:**
<img width="974" height="599" alt="image" src="https://github.com/user-attachments/assets/c3e895c3-f0dd-482e-9406-af06ad727c97" />


**Результат:**

* Найдено: **16 негативных отзывов**

---

### 3. Уникальные слова клиента
**Задача:** Для конкретного клиента (выберите любой ID) выведите список всех уникальных слов, которые он использовал во всех своих отзывах (если отзывов несколько, объедините их).

```sql
-- Выводим уникальные слова для клиента с ID = 2
WITH customer_reviews AS (
    SELECT feedback
    FROM customer_survey
    WHERE customer_id = 2
),
all_words AS (
    SELECT 
        UNNEST(STRING_TO_ARRAY(
            LOWER(REGEXP_REPLACE(feedback, '[^a-zA-Z\s]', '', 'g')), 
            ' '
        )) AS word
    FROM customer_reviews
)
SELECT DISTINCT word
FROM all_words
WHERE LENGTH(word) >= 3
ORDER BY word;
```

**Скриншот результата:**
<img width="222" height="479" alt="image" src="https://github.com/user-attachments/assets/5282689b-d498-49e5-a9d8-d3a4ce5bf796" />


---

## ✅ Итоговые выводы

В ходе выполнения работы:

* Освоены **продвинутые SQL-запросы**
* Выполнен:

  * анализ временных данных
  * геопространственный анализ
  * работа с JSON и массивами
  * текстовая аналитика

