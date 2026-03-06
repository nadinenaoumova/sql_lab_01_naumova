---Задание 1. Посчитать % пропусков в поле street_address (через CASE).

SELECT
    ROUND(
        100.0 * COUNT(CASE WHEN street_address IS NULL THEN 1 END) / COUNT(*),
        2
    ) AS null_percentage
FROM customers;

---Задание 2. Посчитать общую выручку для каждого типа продукта.

SELECT
    p.product_type,
    ROUND(SUM(s.sales_amount)::numeric, 2) AS total_revenue
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_type
ORDER BY total_revenue DESC;

---Задание 3. Найти штаты, где минимальная сумма продажи была выше 315.

SELECT
    c.state,
    MIN(s.sales_amount) AS min_sale_amount
FROM sales s
INNER JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.state
HAVING MIN(s.sales_amount) > 315
ORDER BY min_sale_amount;
