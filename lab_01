---Задание 1
SELECT
    customer_id,
    product_id,
    sales_transaction_date,
    sales_amount,
    channel,
    dealership_id
FROM sales
WHERE sales_transaction_date >= '2018-01-01' 
  AND sales_transaction_date < '2018-02-01'
ORDER BY sales_amount ASC;


---Задание 2
-- Клиенты с IP на '192.', сортировка по фамилии и имени (по возрастанию)
SELECT
    customer_id,
    first_name,
    last_name,
    email,
    ip_address,
    city,
    state
FROM customers
WHERE ip_address LIKE '192.%'
ORDER BY last_name ASC, first_name ASC;


---Задание 3
-- Шаг 1. Создание таблицы female_staff
CREATE TABLE female_staff AS
SELECT
    salesperson_id,
    first_name,
    last_name,
    title,
    hire_date,
    termination_date
FROM salespeople
WHERE gender = 'Female';

-- Проверка создания
SELECT * FROM female_staff;

-- Шаг 2. Обновление титула на 'Ms.'
UPDATE female_staff
SET title = 'Ms.';

-- Проверка обновления
SELECT 
    salesperson_id, 
    first_name, 
    last_name, 
    title, 
    hire_date
FROM female_staff;

-- Шаг 3. Удаление сотрудниц, нанятых до 2015 года
DELETE FROM female_staff
WHERE hire_date < '2015-01-01';

-- Шаг 4. Финальная проверка
SELECT
    salesperson_id,
    first_name,
    last_name,
    title,
    hire_date,
    termination_date
FROM female_staff
ORDER BY hire_date ASC;
