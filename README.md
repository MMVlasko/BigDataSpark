#### Лабораторную работу № 2 выполнил Власко Михаил Михайлович, группа М8О-308Б-23.

Запуск с выполнением ETL и созданием отчётов: `docker compose --profile run-etl up --build`.

Запуск без выполнения ETL и создания отчётов: `docker compose up -d`.

Отчёты реализованы в системе *clickhouse*.

Запуск командной строки clickhouse: `docker exec -it bigdata-clickhouse clickhouse-client --user clickhouse --password clickhouse`.

Список таблиц отчётов:

1. Продажи по продуктам:
   1. Топ-10 самых продаваемых продуктов.
        - `SELECT * FROM top_10_products;`
   2. Общая выручка по категориям продуктов.
        - `SELECT * FROM revenue_by_category;`
   3. Средний рейтинг и количество отзывов для каждого продукта.
        - `SELECT * FROM product_ratings_reviews;`
2. Продажи по клиентам:
   1. Топ-10 клиентов с наибольшей общей суммой покупок.
        - `SELECT * FROM top_10_customers;`
   2. Распределение клиентов по странам.
        - `SELECT * FROM customer_distribution;`
   3. Средний чек для каждого клиента.
        - `SELECT * FROM avg_check_per_customer;`
3. Продажи по времени:
   1. Месячные тренды продаж.
        - `SELECT * FROM monthly_trends;`
   2. Годовые тренды продаж.
        - `SELECT * FROM yearly_trends;`
   3. Сравнение выручки за периоды.
        - `SELECT * FROM monthly_revenue_comparison;`
   4. Средний размер заказа по месяцам.
        - `SELECT * FROM avg_order_size_monthly;`
4. Продажи по магазинам
    1. Топ-5 магазинов с наибольшей выручкой.
        - `SELECT * FROM top_5_stores;`
    2. Распределение продаж по городам.
        - `SELECT * FROM sales_by_city;`
    3. Распределение продаж по странам.
        - `SELECT * FROM sales_by_country;`
    4. Средний чек для каждого магазина.
        - `SELECT * FROM avg_check_per_store;`
5. Продажи по поставщикам:
    1. Топ-5 поставщиков с наибольшей выручкой.
        - `SELECT * FROM top_5_suppliers;`
    2. Средняя цена товаров от каждого поставщика.
        - `SELECT * FROM avg_price_per_supplier;`
    3. Распределение продаж по странам поставщиков.
        - `SELECT * FROM supplier_sales_by_country;`
6. Качество продукции:
    1. Продукты с наивысшим рейтингом.
        - `SELECT * FROM highest_rated_products;`
    2. Продукты с наименьшим рейтингом.
        - `SELECT * FROM lowest_rated_products;`
    3. Корреляция между рейтингом и объёмом продаж.
        - `SELECT * FROM rating_sales_correlation;`
    4. Продукты с наибольшим количеством отзывов.
        - `SELECT * FROM most_reviewed_products;`
