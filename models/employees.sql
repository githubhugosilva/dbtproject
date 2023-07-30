
--model employees


with calc_employees as (

SELECT
    date_part (year, getdate()) - date_part (year, A.birth_date) AS age,   
    date_part (year, getdate()) - date_part (year, A.Hire_date) AS lengthofservice,   
    FIRST_NAME ||' '|| LAST_NAME AS name, *
FROM {{source('sources','employees')}} AS A

) 
select * from calc_employees