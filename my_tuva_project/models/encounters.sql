{{ config(
    materialized='table',
    alias='standardized_encounters'  
) }}

with raw_encounters as (
    select * from {{ source('medical_data', 'raw_encounters_data') }}
)

select
    id as encounter_id,
    start as encounter_start_date,
    stop as encounter_end_date,
    patient as patient_id,
    code as encounter_code,
    current_timestamp as standardized_at
from raw_encounters