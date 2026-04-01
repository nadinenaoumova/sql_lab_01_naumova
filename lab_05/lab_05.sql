---Задание 1
---Анализ до создания индекса
EXPLAIN (ANALYZE, BUFFERS, VERBOSE) 
SELECT * FROM dealerships 
WHERE date_opened = '2010-01-01 00:00:00';

---Задание 2
---Создаем B-Tree индекс
CREATE INDEX idx_dealerships_date_opened ON dealerships (date_opened);

---Смотрим результаты после создания индекса
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT e.email_id, e.email_subject, e.opened_date, 
       c.first_name || ' ' || c.last_name AS customer_name
FROM emails e
JOIN customers c ON e.customer_id = c.customer_id
WHERE e.email_subject = 'A New Year, And Some New EVs';


---Задание 3
-- Создаем B-Tree индекс на поле email_subject
CREATE INDEX idx_emails_subject ON emails (email_subject);

-- Обновляем статистику
ANALYZE emails;

---Смотрим результаты
EXPLAIN (ANALYZE, BUFFERS, VERBOSE)
SELECT e.email_id, e.email_subject, e.opened_date, 
       c.first_name || ' ' || c.last_name AS customer_name
FROM emails e
JOIN customers c ON e.customer_id = c.customer_id
WHERE e.email_subject = 'A New Year, And Some New EVs';

