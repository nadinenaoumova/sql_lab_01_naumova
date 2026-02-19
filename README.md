# sql_lab_01_naumova
Лабораторная работа №1 по SQL, вариант 14
# Лабораторная работа №1: Формирование SQL-запросов (SELECT, CRUD)

**Вариант:** 14  
**Студент:** Наумова Надя 

---

## Задание 1 (Основной сервер): Продажи в январе 2018

**Текст задания:** Продажи (sales) в январе 2018 года. Сортировка: сумма (по возрастанию).

**SQL-запрос:**
```sql
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

Скриншот <img width="974" height="674" alt="image" src="https://github.com/user-attachments/assets/486380e8-faaf-4246-8354-c4945861019d" />



