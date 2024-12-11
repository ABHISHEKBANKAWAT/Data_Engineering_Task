

with raw_symptoms as (
    select * from {{ source('MedicalData', 'raw_symptoms_data') }}
)

select
    -- Renamed columns
    num_symptoms as num_observations,
    -- Symtom1 as obsersation1,
    -- Symtom2 as obsersation2,
    -- Symtom3 as obsersation3,
    -- Symtom4 as obsersation4,
    -- patient as patient_id,
    
    -- Preserve any other original columns
    current_timestamp as standardized_at
from raw_symptoms