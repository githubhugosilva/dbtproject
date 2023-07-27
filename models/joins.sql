
WITH TB_prod as (
    SELECT 
        ct.category_name
        , sp.company_name as suppliers
        , pd.product_name
        , pd.unit_price
        , pd.product_id
    FROM {{source('sources','products')}}  AS PD
    LEFT JOIN {{source('sources','suppliers')}} AS SP ON PD.supplier_ID = SP.supplier_ID 
    LEFT JOIN {{source('sources','categories')}} AS CT ON PD.category_id = CT.category_id 
), 

TB_orddetai as 
(
    SELECT PD.*, OD.ORDER_ID, OD.QUANTITY, OD.desconto
    FROM {{ref('orderdetails')}}  AS OD 
    LEFT JOIN TB_prod as pd on od.product_id = pd.product_id
),

TB_ORDRS AS 
(

    select 
        ord.order_date
        ,ord.order_id

    FROM {{source('sources','orders')}}  AS ORD
    LEFT JOIN {{ref('customers')}}  AS CS on (ord.customer_id = cs.customer_id)
    LEFT JOIN {{ref('customers')}}  AS EM on (ord.employee_id = em.employees)
    LEFT JOIN {{source('sources','orders')}}  AS SH ON (ord.ship_via = shipper_id)
)

SELECT * FROM TB_ORDRS