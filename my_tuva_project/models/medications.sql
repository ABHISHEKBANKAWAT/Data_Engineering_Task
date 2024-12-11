

with raw_medications as (
    select * from {{ source('MedicalData', 'raw_medications_data') }}
)

select
    -- Renamed columns
    start as start_datetime,
    "end" as end_datetime,
    patient as patient_id,
    encounter as encounter_id,
    code as medication_id,
    
    -- Preserve any other original columns
    current_timestamp as standardized_at
from raw_medications