БЛОК А
---1. Дни недели продаж. Определите, в какой день недели (понедельник, вторник и т.д.) совершается наибольшее количество продаж (sales).
-- Определяем день недели с наибольшим количеством продаж
SELECT 
    TO_CHAR(sales_transaction_date, 'Day') AS day_of_week,
    COUNT(*) AS transaction_count
FROM sales
GROUP BY 
    TO_CHAR(sales_transaction_date, 'Day'),
    EXTRACT(DOW FROM sales_transaction_date)
ORDER BY transaction_count DESC
LIMIT 1;

---2. Скорость реакции. Рассчитайте среднее время (интервал), которое проходит между регистрацией клиента (date_added в customers) и его первой покупкой (sales_transaction_date в sales).
-- Среднее время от регистрации до первой покупки
WITH first_purchase AS (
    -- Находим дату первой покупки для каждого клиента
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

---3. Сезонность продуктов. Для каждого типа продукта (product_type) определите месяц, в котором он продается лучше всего (максимальная сумма продаж).
-- Задание 3: Лучший месяц продаж для каждого продукта (с названием модели)
WITH monthly_sales AS (
    SELECT 
        s.product_id,
        p.model AS product_model,
        EXTRACT(YEAR FROM s.sales_transaction_date) AS year,
        EXTRACT(MONTH FROM s.sales_transaction_date) AS month,
        SUM(s.sales_amount) AS total_sales
    FROM sales s
    LEFT JOIN products p ON s.product_id = p.product_id
    GROUP BY s.product_id, p.model, EXTRACT(YEAR FROM s.sales_transaction_date), EXTRACT(MONTH FROM s.sales_transaction_date)
),
ranked_sales AS (
    SELECT 
        product_id,
        product_model,
        year,
        month,
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY total_sALES DESC) AS rank
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

БЛОК Б
---1. Ближайший дилер. Для каждого клиента из города 'New York City' найдите ближайший дилерский центр (dealerships) и расстояние до него.
SELECT DISTINCT ON (c.customer_id)
    c.customer_id,
    d.dealership_id,
    d.city AS dealer_city,
    ROUND(CAST((point(c.longitude, c.latitude) <@> point(d.longitude, d.latitude)) AS numeric), 2) AS distance_miles
FROM customers c
CROSS JOIN (SELECT * FROM dealerships LIMIT 500) d
WHERE c.city = 'New York City'
ORDER BY c.customer_id, distance_miles;

---2. Покрытие дилеров. Найдите дилерский центр, у которого наибольшее количество клиентов в радиусе 100 миль.
-- Находим дилерский центр, у которого наибольшее количество клиентов в радиусе 100 миль
WITH dealer_coverage AS (
    SELECT 
        d.dealership_id,
        d.city AS dealer_city,
        COUNT(DISTINCT c.customer_id) AS customers_within_100_miles
    FROM dealerships d
    CROSS JOIN customers c
    WHERE (point(d.longitude, d.latitude) <@> point(c.longitude, c.latitude)) <= 100  -- расстояние <= 100 миль
    GROUP BY d.dealership_id, d.city
)
SELECT 
    dealership_id,
    dealer_city,
    customers_within_100_miles
FROM dealer_coverage
ORDER BY customers_within_100_miles DESC
LIMIT 1;

---3. География модели. Для модели 'Model Chi' найдите среднюю широту и долготу покупателей (центроид продаж).
-- Для модели 'Model Chi' находим среднюю широту и долготу покупателей (центроид продаж)
SELECT 
    p.model AS product_model,
    AVG(c.latitude) AS avg_latitude,
    AVG(c.longitude) AS avg_longitude,
    -- Опционально: можно преобразовать в географическую точку
    ll_to_earth(AVG(c.latitude), AVG(c.longitude)) AS centroid_earth
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN customers c ON s.customer_id = c.customer_id
WHERE p.model = 'Model Chi'
GROUP BY p.model;

БЛОК В
---1. История покупок в JSON. Создайте представление или запрос, который формирует JSON-объект для каждого клиента: { "id": 1, "name": "Ivan", "products": ["Car", "Scooter"] }, используя агрегацию массивов.
-- Формируем JSON-объект для каждого клиента с массивом купленных продуктов
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
ORDER BY c.customer_id

---2. Агрегация в массив. Сгруппируйте данные по штатам (state) и сформируйте массив email-адресов всех дилеров в этом штате.
-- Группируем дилеров по штатам и формируем массив email-адресов из таблицы contacts
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

---3. Тэгирование. Добавьте к таблице customers текстовое поле-массив tags. Напишите запрос, который добавляет тег 'VIP' всем клиентам, совершившим покупки на сумму более 50000.
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

БЛОК Г
---1. Частотный словарь. Составьте топ-10 самых часто встречающихся слов в таблице customer_survey (столбец feedback), исключив слова короче 3 символов.
-- Топ-10 самых частотных слов в отзывах (исключая слова короче 3 символов)
WITH all_words AS (
    -- Разбиваем текст отзывов на отдельные слова
    SELECT 
        LOWER(REGEXP_REPLACE(feedback, '[^a-zA-Zа-яА-Я\s]', '', 'g')) AS cleaned_feedback
    FROM customer_survey
    WHERE feedback IS NOT NULL
),
words_split AS (
    -- Превращаем каждое слово в отдельную строку
    SELECT 
        UNNEST(STRING_TO_ARRAY(cleaned_feedback, ' ')) AS word
    FROM all_words
)
SELECT 
    word,
    COUNT(*) AS frequency
FROM words_split
WHERE LENGTH(word) >= 3  -- исключаем слова короче 3 символов
  AND word NOT IN ('the', 'and', 'for', 'was', 'with', 'that', 'have', 'this', 'but', 'are', 'not', 'you', 'all', 'can', 'has', 'had', 'were', 'from', 'they', 'she', 'he', 'will', 'one', 'have', 'been', 'his', 'her', 'their', 'your', 'its', 'our', 'out', 'get', 'has', 'him', 'her', 'for', 'not', 'are', 'was', 'were', 'this', 'that', 'these', 'those', 'some', 'any', 'into', 'than', 'then', 'there', 'their', 'they', 'would', 'could', 'should', 'what', 'when', 'where', 'which', 'while', 'with', 'within', 'without', 'after', 'before', 'over', 'under', 'between', 'through', 'during', 'without', 'about', 'again', 'never', 'always', 'every', 'other', 'another', 'such', 'same', 'different', 'each', 'both', 'all', 'most', 'more', 'very', 'just', 'but', 'so', 'too', 'also', 'only', 'even', 'back', 'here', 'there', 'where', 'when', 'why', 'how', 'then', 'than', 'then', 'well', 'now', 'then', 'being', 'than', 'then')
GROUP BY word
ORDER BY frequency DESC
LIMIT 10;

---2. Поиск негатива. Найдите все отзывы, содержащие слова с корнем 'bad', 'fail', 'poor' (используйте to_tsvector и plainto_tsquery или ILIKE).
-- Поиск негативных отзывов 
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

---3. Уникальные слова. Для конкретного клиента (выберите любой ID) выведите список всех уникальных слов, которые он использовал во всех своих отзывах (если отзывов несколько, объедините их).
-- Выводим уникальные слова для клиента с ID = 2
WITH customer_reviews AS (
    SELECT feedback
    FROM customer_survey
    WHERE customer_id = 2
      AND feedback IS NOT NULL
),
all_words AS (
    SELECT 
        UNNEST(STRING_TO_ARRAY(
            LOWER(REGEXP_REPLACE(feedback, '[^a-zA-Z\s]', '', 'g')), 
            ' '
        )) AS word
    FROM customer_reviews
)
SELECT DISTINCT word AS unique_word
FROM all_words
WHERE LENGTH(word) >= 3
  AND word != ''
ORDER BY word;

