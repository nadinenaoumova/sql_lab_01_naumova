--Задание 1: Ранжировать сотрудников по фамилии внутри гендерных групп (gender).
SELECT 
    salesperson_id,
    first_name,
    last_name,
    gender,
    RANK() OVER (
        PARTITION BY gender 
        ORDER BY last_name
    ) AS rank_by_last_name
FROM salespeople;


--Задание 2: Вычислить прирост продаж (sales_amount - LAG(sales_amount)) по дням.
SELECT 
    sales_transaction_date::DATE AS sale_date,
    SUM(sales_amount) AS daily_sum,
    SUM(sales_amount) - LAG(SUM(sales_amount)) OVER (ORDER BY sales_transaction_date::DATE) AS sales_growth
FROM sales
GROUP BY sales_transaction_date::DATE
ORDER BY sale_date;


--Задание 3: Средняя цена (base_msrp) продуктов накопительным итогом при сортировке по дате начала производства.
SELECT 
    product_id,
    model,
    production_start_date,
    base_msrp,
    AVG(base_msrp) OVER (
        ORDER BY production_start_date
    ) AS running_avg_price
FROM products
ORDER BY production_start_date;
