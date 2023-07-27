/*
SELECT *, ROW_NUMBER () OVER( partition BY company_name ORDER BY company_name)
FROM {{source('sources','customers')}}
*/

with TB_markup as
(
    select * ,
    first_value(customer_id) over (partition by company_name, contact_name order by company_name rows between unbounded preceding and unbounded following ) as result
FROM {{source('sources','customers')}}
), 
TB_removed as 
(
    select distinct result from TB_markup
), 

TB_final as 
(
    SELECT * 
    FROM {{source('sources','customers')}}
    WHERE customer_id IN (SELECT result FROM TB_removed)
)

select * from TB_final