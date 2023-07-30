WITH orderdetails as (
SELECT

    od.order_id
    , od.product_id
    , od.unit_price
    , od.quantity
    , pr.product_name
    , pr.supplier_id
    , pr.category_id
    , od.unit_price * od.quantity as total
    , pr.unit_price * od.quantity - total as desconto

FROM {{source('sources','order_details')}} as od 
LEFT JOIN {{source('sources','products')}} as pr on od.product_id = pr.product_id
)
SELECT * FROM orderdetails