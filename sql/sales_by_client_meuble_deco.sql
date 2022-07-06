-- POSTGRESQL

SELECT t.client_id,
	   -- SUM(t.prod_price * t.prod_qty) AS ventes,
	   SUM(CASE WHEN p.product_type = 'MEUBLE' THEN t.prod_price * t.prod_qty ELSE 0 END) AS ventes_meuble,
	   SUM(CASE WHEN p.product_type = 'DECO' THEN t.prod_price * t.prod_qty ELSE 0 END) AS ventes_deco
FROM public."TRANSACTION" AS t JOIN public."PRODUCT_NOMENCLATURE" AS p ON t.prop_id = p.product_id
WHERE t.date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY t.client_id;
ORDER BY t.date ASC;