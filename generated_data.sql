-- 1. Переключаемся на схему orders
SET search_path TO orders;

-- 2. Очищаем таблицы, если в них что-то было (с каскадным удалением зависимостей)
TRUNCATE order_events, orders, customers, drivers, locations, tariffs, order_statuses RESTART IDENTITY CASCADE;

-- 3. Заполняем базовые справочники статическими данными
INSERT INTO order_statuses (name) VALUES
('Создан'), ('Водитель назначен'), ('Машина подъехала'), ('В пути'), ('Завершен'), ('Отменен');

INSERT INTO tariffs (name, base_price, per_km, created_at) VALUES
('Эконом', 150.00, 12.00, '2026-01-01 00:00:00+00'),
('Комфорт', 250.00, 18.00, '2026-01-01 00:00:00+00'),
('Бизнес', 450.00, 30.00, '2026-01-01 00:00:00+00'),
('Минивэн', 350.00, 22.00, '2026-01-15 12:00:00+00'),
('Доставка', 100.00, 10.00, '2026-02-01 08:30:00+00');

-- 4. Генерируем 5 000 локаций (в пределах Москвы и области)
INSERT INTO locations (address, lat, lon)
SELECT
    'ул. Генерации, д. ' || i AS address,
    (55.500000 + random() * (55.900000 - 55.500000))::numeric(9,6) AS lat,
    (37.300000 + random() * (37.900000 - 37.300000))::numeric(9,6) AS lon
FROM generate_series(1, 5000) AS i;

-- 5. Генерируем 10 000 клиентов
INSERT INTO customers (external_id, name, phone, email, created_at)
SELECT
    'cust_ext_' || i AS external_id,
    'Клиент ' || i AS name,
    '+7999' || lpad(i::text, 7, '0') AS phone,
    'user_' || i || '@example.com' AS email,
    '2026-01-01 00:00:00+00'::timestamp with time zone + random() * (interval '45 days') AS created_at
FROM generate_series(1, 10000) AS i;

-- 6. Генерируем 10 000 водителей
INSERT INTO drivers (external_id, name, phone, vehicle_number, created_at)
SELECT
    'drv_ext_' || i AS external_id,
    'Водитель ' || i AS name,
    '+7911' || lpad(i::text, 7, '0') AS phone,
    -- Гарантированно уникальная комбинация на основе переменной i:
    (ARRAY['А','В','Е','К','М','Н','О','Р','С','Т','У','Х'])[(i % 12) + 1] ||
    lpad(((i * 7) % 1000)::text, 3, '0') ||
    (ARRAY['А','В','Е','К','М','Н','О','Р','С','Т','У','Х'])[((i / 12) % 12) + 1] ||
    (ARRAY['А','В','Е','К','М','Н','О','Р','С','Т','У','Х'])[((i / 144) % 12) + 1] ||
    '66' AS vehicle_number,
    '2026-01-01 00:00:00+00'::timestamp with time zone + random() * (interval '30 days') AS created_at
FROM generate_series(1, 10000) AS i;

-- 7. Генерируем 100 000 заказов (Таблица фактов)
-- Мы используем временную таблицу или подзапрос, чтобы рассчитать логичные даты и цены
INSERT INTO orders (
    external_order_id, customer_id, driver_id,
    pickup_location_id, dropoff_location_id, tariff_id,
    price, distance_km, created_at, delivered_at, current_status
)
-- Чтобы увеличить количество заказов до 1 000 000, просто поменяйте 100000 на 1000000 ниже
SELECT
    'order_ext_' || i AS external_order_id,
    -- Случайный клиент из существующих (1..10000)
    floor(random() * 10000 + 1)::int AS customer_id,
    -- 90% заказов имеют водителя, 10% (например, отмененные) без водителя
    CASE WHEN random() > 0.1 THEN floor(random() * 2000 + 1)::int ELSE NULL END AS driver_id,
    -- Случайные адреса А и Б
    floor(random() * 5000 + 1)::int AS pickup_location_id,
    floor(random() * 5000 + 1)::int AS dropoff_location_id,
    -- Случайный тариф (1..5)
    floor(random() * 5 + 1)::int AS tariff_id,
    -- Случайная цена от 150 до 2500 руб.
    (150 + random() * 2350)::numeric(10,2) AS price,
    -- Случайная дистанция от 1 до 60 км
    (1 + random() * 59)::numeric(7,3) AS distance_km,
    -- Дата создания заказа (в течение февраля и марта 2026)
    created_date AS created_at,
    -- Если заказ завершен (статус 5), добавляем время поездки от 10 до 60 минут
    CASE WHEN status_id = 5 THEN created_date + (10 + random() * 50) * interval '1 minute' ELSE NULL END AS delivered_at,
    status_id AS current_status
FROM (
    SELECT
        i,
        '2026-02-01 00:00:00+00'::timestamp with time zone + random() * (interval '45 days') AS created_date,
        -- Распределение статусов: 85% завершены (5), 10% отменены (6), 5% в других статусах (1..4)
        CASE
            WHEN rand < 0.85 THEN 5
            WHEN rand < 0.95 THEN 6
            ELSE floor(random() * 4 + 1)::int
        END AS status_id
    FROM (
        SELECT i, random() AS rand
        FROM generate_series(1, 100000) AS i
    ) AS base
) AS orders_generated;


-- 8. Генерируем историю событий (order_events) для созданных заказов
-- Для каждого заказа создадим базовое событие "Создан", а для завершенных добавим финальный статус
INSERT INTO order_events (order_id, status_id, event_time, note)
SELECT
    order_id,
    1 AS status_id,
    created_at AS event_time,
    'Заказ зарегистрирован в системе' AS note
FROM orders;

-- Добавляем финальные статусы для завершенных и отмененных (чтобы оживить логи)
INSERT INTO order_events (order_id, status_id, event_time, note)
SELECT
    order_id,
    current_status AS status_id,
    COALESCE(delivered_at, created_at + interval '5 minutes') AS event_time,
    CASE
        WHEN current_status = 5 THEN 'Заказ успешно выполнен'
        WHEN current_status = 6 THEN 'Заказ отменен'
        ELSE 'Статус обновлен'
    END AS note
FROM orders
WHERE current_status IN (5, 6);

-- 9. Запускаем сбор статистики, чтобы планировщик запросов (Query Planner) работал корректно
ANALYZE;
