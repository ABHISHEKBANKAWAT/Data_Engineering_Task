

with raw_patients as (
    select * from {{ source('MedicalData', 'raw_patients_data') }}
)

select
    -- Renamed columns
    first as first_name,
    maiden as maiden_name,
    last as last_name,
    income as annual_income,
    gender as sex,
    birthdate as birth_date,
    ssn as social_security_number,
    zip as zipcode,
    lat as latitude,
    lon as longitude,
    
    -- Preserve any other original columns
    -- Add any additional transformations or standardizations here
    current_timestamp as standardized_at
from raw_patients