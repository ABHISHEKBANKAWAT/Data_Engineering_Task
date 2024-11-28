import pandas as pd

def _remove_digits_from_names(x: pd.Series) -> pd.Series:
    """Removes digits from strings in name columns."""
    return x.str.replace(r'\d+', '', regex=True)

def _fix_negative_income(x: pd.Series) -> pd.Series:
    """Removes negative signs in the income column."""
    return x.abs()

def _rename_and_format_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renames columns for consistency and formats datetime and numeric columns."""
    df['startdate'] = pd.to_datetime(df['start'], errors='coerce')
    df['stopdate'] = pd.to_datetime(df['stop'], errors='coerce')
    df.drop(columns=['start', 'stop'], inplace=True)
    df.rename(columns={'startdate': 'start', 'stopdate': 'stop'}, inplace=True)
    return df

def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate rows based on patient and encounter columns."""
    return df.drop_duplicates(subset=['patient', 'encounter'])

def _fix_reasoncode_format(df: pd.DataFrame) -> pd.DataFrame:
    """Fixes formatting issues in the reasoncode column."""
    df['reasoncode'] = df['reasoncode'].str.strip().str.upper()  # Standardizing format
    return df

def _fill_missing_gender(df: pd.DataFrame, patient_gender_data: pd.DataFrame) -> pd.DataFrame:
    """Fills missing gender values from the patient_gender_data."""
    df = df.merge(patient_gender_data[['patient_id', 'gender']], how='left', on='patient_id', suffixes=('', '_filled'))
    df['gender'] = df['gender'].fillna(df['gender_filled'])
    df.drop(columns=['gender_filled'], inplace=True)  # Drop the filled column after merging
    return df

def preprocess_raw_patient_data(raw_patient_data: pd.DataFrame, raw_patient_gender_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses raw patient data."""
    raw_patient_data['first_name'] = _remove_digits_from_names(raw_patient_data['first_name'])
    raw_patient_data['last_name'] = _remove_digits_from_names(raw_patient_data['last_name'])
    raw_patient_data['maiden_name'] = _remove_digits_from_names(raw_patient_data['maiden_name'])
    raw_patient_data['income'] = _fix_negative_income(raw_patient_data['income'])
    raw_patient_data = _fill_missing_gender(raw_patient_data, raw_patient_gender_data)  # Fill missing gender
    return raw_patient_data

def preprocess_raw_conditions_data(raw_conditions_data: pd.DataFrame, raw_patient_gender_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses raw conditions data."""
    raw_conditions_data = _rename_and_format_columns(raw_conditions_data)
    raw_conditions_data = _remove_duplicates(raw_conditions_data)
    raw_conditions_data = _fix_reasoncode_format(raw_conditions_data)
    raw_conditions_data = _fill_missing_gender(raw_conditions_data, raw_patient_gender_data)  # Fill missing gender
    return raw_conditions_data

def preprocess_raw_encounters_data(raw_encounters_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses raw encounters data."""
    raw_encounters_data['date'] = pd.to_datetime(raw_encounters_data['date'], errors='coerce')
    raw_encounters_data['encounter_code'] = raw_encounters_data['encounter_code'].str.strip()
    raw_encounters_data.rename(columns={'id': 'ENCOUNTER_ID', 'code': 'encounter_code'}, inplace=True)
    raw_encounters_data = _fix_reasoncode_format(raw_encounters_data)
    return raw_encounters_data

def preprocess_raw_symptoms_data(raw_symptoms_data: pd.DataFrame, raw_patient_gender_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses raw symptoms data."""
    raw_symptoms_data = raw_symptoms_data[raw_symptoms_data['condition'] == 'lupus']  # Only lupus patients
    raw_symptoms_data = _remove_duplicates(raw_symptoms_data)
    raw_symptoms_data.rename(columns={'patient_id': 'patient_ID', 'gender': 'patient_gender'}, inplace=True)
    raw_symptoms_data = _fill_missing_gender(raw_symptoms_data, raw_patient_gender_data)  # Fill missing gender
    raw_symptoms_data.drop(columns=['patient_gender'], inplace=True)  # Empty gender column removal
    return raw_symptoms_data

def preprocess_raw_medications_data(raw_medications_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses raw medications data."""
    raw_medications_data['date'] = pd.to_datetime(raw_medications_data['date'], errors='coerce')
    raw_medications_data['reason_description'] = raw_medications_data['reason_description'].str.strip()
    raw_medications_data = _remove_duplicates(raw_medications_data)
    raw_medications_data = raw_medications_data.dropna(subset=['reasoncode', 'stopdate'])  # Removing rows with missing reasoncode or stopdate
    return raw_medications_data

# Main function that will be called in Kedro pipeline
def process_datasets(raw_patient_data, raw_conditions_data, raw_encounters_data, raw_medications_data, raw_symptoms_data, raw_patient_gender_data):
    """Processes all raw data and returns cleaned datasets."""
    cleaned_patient_data = preprocess_raw_patient_data(raw_patient_data, raw_patient_gender_data)
    cleaned_conditions_data = preprocess_raw_conditions_data(raw_conditions_data, raw_patient_gender_data)
    cleaned_encounters_data = preprocess_raw_encounters_data(raw_encounters_data)
    cleaned_symptoms_data = preprocess_raw_symptoms_data(raw_symptoms_data, raw_patient_gender_data)
    cleaned_medications_data = preprocess_raw_medications_data(raw_medications_data)

    # You can also save the cleaned dataframes here if required
    cleaned_patient_data.to_csv(r"C:\Users\ABHISHEK BANKAWAT\Desktop\Data_Engineering_Task\step2-pipeline\data\02_intermediate\cleaned_patient_data.csv", index=False)
    cleaned_conditions_data.to_csv(r"C:\Users\ABHISHEK BANKAWAT\Desktop\Data_Engineering_Task\step2-pipeline\data\02_intermediate\cleaned_conditions_data.csv", index=False)
    # ...

    return cleaned_patient_data, cleaned_conditions_data, cleaned_encounters_data, cleaned_symptoms_data, cleaned_medications_data
