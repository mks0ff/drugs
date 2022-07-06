-- POSTGRESQL

CREATE TABLE IF NOT EXISTS public."TRANSACTION"
(
    date date,
    order_id character varying,
    client_id character varying,
    prop_id character varying,
    prod_price double precision,
    prod_qty integer
);

CREATE TABLE IF NOT EXISTS public."PRODUCT_NOMENCLATURE"
(
    product_id character varying,
    product_type character varying,
    product_name character varying
);

insert into public."TRANSACTION" values ('01/01/20', '1234' , '999' , '490756' , 50 , 1);
insert into public."TRANSACTION" values ('01/01/20', '1234' , '999' , '389728' , 3.56 , 4);
insert into public."TRANSACTION" values ('01/01/20', '3456' , '845' , '490756' , 50 , 2);
insert into public."TRANSACTION" values ('01/01/20', '3456' , '845' , '549380' , 300 , 1);
insert into public."TRANSACTION" values ('01/01/20', '3456' , '845' , '293718' , 10 , 6);

insert into public."PRODUCT_NOMENCLATURE" values ('490756', 'MEUBLE', 'Chaise');
insert into public."PRODUCT_NOMENCLATURE" values ('389728', 'DECO', 'Boule de Noël');
insert into public."PRODUCT_NOMENCLATURE" values ('549380', 'MEUBLE', 'Canapé');
insert into public."PRODUCT_NOMENCLATURE" values ('293718', 'DECO', 'Mug');