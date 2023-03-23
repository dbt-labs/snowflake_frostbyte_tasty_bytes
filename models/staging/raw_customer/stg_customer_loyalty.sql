{{ config(materialized="view", sort="customer_id", dist="customer_id") }}

with
    source as (
        select customer_id, first_name, last_name, city, country, postal_code, preferred_language, gender, favourite_brand, marital_status, children_count, sign_up_date, birthday_date, e_mail, phone_number
        from {{ source("raw_customer", "customer_loyalty") }}
    ),
    renamed as (
        select
            customer_id as customer_id,
            first_name as first_name,
            last_name as last_name,
            city as city,
            country as country,
            postal_code as postal_code,
            preferred_language as preferred_language,
            gender as gender,
            favourite_brand as favourite_brand,
            marital_status as marital_status,
            children_count as children_count,
            sign_up_date as sign_up_date,
            birthday_date as birthday_date,
            e_mail as e_mail,
            phone_number as phone_number
        from source
    )
select *
from renamed