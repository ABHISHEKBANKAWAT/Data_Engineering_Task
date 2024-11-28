import pandas as pd

# Function to clean patient data
def clean_patient_data(df: pd.DataFrame, gender_data: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    df['firstname'] = df['firstname'].str.replace(r'\d+', '', regex=True)
    df['maidenname'] = df['maidenname'].str.replace(r'\d+', '', regex=True)
    df['lastname'] = df['lastname'].str.replace(r'\d+', '', regex=True)
    df['income'] = df['income'].abs()
    df = df.merge(gender_data, on='patient_id', how='left')
    
    return df

# Function to clean conditions data
def clean_conditions_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    df['description'] = df['description'].str.lower()
    df.rename(columns={
        'start': 'start_date',
        'end': 'end_date',
        'code': 'condition_code',
        'patient': 'patient_id',
        'encounter': 'encounter_id'
    }, inplace=True)
    
    return df

# Function to clean encounters data
def clean_encounters_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    df.rename(columns={
        'id': 'encounter_id',
        'start': 'start_datetime',
        'end': 'end_datetime',
        'patient': 'patient_id',
        'code': 'encounter_code'
    }, inplace=True)
    
    return df

# Function to clean symptoms data
def clean_symptoms_data(df: pd.DataFrame, gender_data: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    df.rename(columns={'patient': 'patient_id'}, inplace=True)
    df = df.merge(gender_data, on='patient_id', how='left')
    
    return df

# Function to clean medication data
def clean_medication_data(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower()
    df.rename(columns={
        'start': 'start_datetime',
        'end': 'end_datetime',
        'patient': 'patient_id',
        'encounter': 'encounter_id',
        'code': 'medication_code'
    }, inplace=True)
    
    return df
