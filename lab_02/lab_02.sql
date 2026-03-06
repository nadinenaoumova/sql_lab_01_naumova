Задание 1. Вывести первых 15 клиентов (имя, город) и дату отправки им писем.

SELECT
    c.first_name,
    c.last_name,
    c.city,
    e.sent_date
FROM customers c
INNER JOIN emails e ON c.customer_id = e.customer_id
ORDER BY c.customer_id, e.sent_date DESC
LIMIT 15;


Задание 2. Найдите 10 дилеров, у которых меньше всего сотрудников.

SELECT
    d.dealership_id,
    d.city,
    d.state,
    COUNT(s.salesperson_id) AS employees
FROM dealerships d
LEFT JOIN salespeople s ON d.dealership_id = s.dealership_id
GROUP BY d.dealership_id, d.city, d.state
ORDER BY employees
LIMIT 10;


Задание 3. Рассчитайте разницу между base_msrp и sales_amount. Если результат отрицательный, вывести 0 (использовать GREATEST).

SELECT
    s.customer_id,
    s.product_id,
    s.sales_transaction_date,
    p.model,
    p.base_msrp,
    s.sales_amount,
    ROUND(CAST(GREATEST(p.base_msrp - s.sales_amount, 0) AS numeric), 2) AS price_difference
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
ORDER BY s.sales_transaction_date;
