.print ===== 1 moyennes =====
SELECT
  round((SELECT count(*) FROM orders) / (SELECT count(*) FROM users), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

.print ===== 2 premiere et derniere commande =====
SELECT
  u.username,
  min(o.date) AS first_order,
  max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM users u
LEFT JOIN orders o ON o.user_uuid = u.uuid
GROUP BY u.uuid, u.username
ORDER BY first_order
LIMIT 10;

.print ===== 3 heure la plus vendeuse =====
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
QUALIFY rank() OVER (ORDER BY sum(quantity) DESC) = 1;

.print ===== 4 variation mensuelle par produit =====
WITH monthly AS (
  SELECT product, date_trunc('month', date) AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY product, month
),
compared AS (
  SELECT product, month, quantity,
    lag(quantity) OVER (PARTITION BY product ORDER BY month) AS previous
  FROM monthly
)
SELECT product, strftime(month, '%Y-%m') AS month, quantity, previous,
  round(100.0 * (quantity - previous) / previous, 1) AS variation_pct
FROM compared
ORDER BY product, month;

.print ===== 5 users ayant commande tous les produits =====
SELECT u.username, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.uuid, u.username
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
