-- POSTGRESQL

SELECT t.date, SUM(t.prod_qty * t.prod_price) AS ventes
FROM public."TRANSACTION" as t
WHERE t.date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY t.date
ORDER BY t.date ASC;

