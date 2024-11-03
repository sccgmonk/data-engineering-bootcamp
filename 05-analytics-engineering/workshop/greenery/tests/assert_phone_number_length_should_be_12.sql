select
    length(phone_number)

from {{ ref('my_users') }} as len_phone
where length(phone_number) != 12