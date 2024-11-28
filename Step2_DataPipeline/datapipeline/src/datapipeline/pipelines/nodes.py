import pandas as pd


# Helper Functions
def _remove_digits(x: pd.Series) -> pd.Series:
    """Removes digits from strings in a Series."""
    return x.str.replace(r"\d+", "", regex=True)


def _to_lowercase(x: pd.Series) -> pd.Series:
    """Converts all strings in a Series to lowercase."""
    return x.str.lower()


def _parse_absolute(x: pd.Series) -> pd.Series:
    """Converts values in a Series to integers and then to their absolute values."""
    return x.astype(int).abs()


def _standardize_column_names(dataset: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Standardizes column names for a given dataset according to the provided mapping."""
    return dataset.rename(columns=column_mapping)


def extract_symptoms(symptoms_table: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts individual symptoms from the `SYMPTOMS` column into separate columns.

    Args:
        symptoms_table (pd.DataFrame): The Symptoms table containing the `SYMPTOMS` column.

    Returns:
        pd.DataFrame: A modified Symptoms table with extracted symptoms in separate columns.
    """
    symptoms_table[['Symptom1', 'Symptom2', 'Symptom3', 'Symptom4']] = symptoms_table['symptoms'].str.split('|', expand=True)
    symptoms_table.drop(columns=['symptoms'], inplace=True)
    return symptoms_table


# Preprocessing Functions
def preprocess_patient_data(patient_data: pd.DataFrame, gender_data: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Preprocesses the patient dataset."""
    patient_data.columns = patient_data.columns.str.lower()
    patient_data["first"] = _remove_digits(patient_data["first"])
    patient_data["maiden"] = _remove_digits(patient_data["maiden"])
    patient_data["last"] = _remove_digits(patient_data["last"])
    patient_data["income"] = _parse_absolute(patient_data["income"])
    patient_data = _standardize_column_names(patient_data, column_mapping)
    patient_data.drop(columns=['gender'], inplace=True)
    patient_data = patient_data.merge(gender_data, left_on="patient_id", right_on="id", how="left")
    patient_data.columns = patient_data.columns.str.lower()
    patient_data.drop(columns=['id'], inplace=True)
    return patient_data


def preprocess_conditions_data(conditions: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Preprocesses the conditions dataset."""
    conditions.columns = conditions.columns.str.lower()
    conditions = _standardize_column_names(conditions, column_mapping)
    conditions["description"] = _to_lowercase(conditions["description"])
    conditions.rename(columns={"patient": "patient_id"}, inplace=True)
    return conditions


def preprocess_encounters_data(encounters: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Preprocesses the encounters dataset."""
    encounters.columns = encounters.columns.str.lower()
    encounters = _standardize_column_names(encounters, column_mapping)
    encounters.rename(columns={"patient": "patient_id"}, inplace=True)
    return encounters


def preprocess_symptoms_data(symptoms: pd.DataFrame, gender_data: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Preprocesses the symptoms dataset and standardizes column names based on the Tuva Data Model."""
    symptoms.columns = symptoms.columns.str.lower()
    
    # Standardize column names using the provided column_mapping
    symptoms = _standardize_column_names(symptoms, column_mapping)
    
    # Drop unnecessary columns and merge with gender data
    symptoms.drop(columns=['gender'], inplace=True)
    symptoms = symptoms.merge(gender_data, left_on="patient_id", right_on="id", how="left")
    symptoms.columns = symptoms.columns.str.lower()
    symptoms.drop(columns=['id'], inplace=True)
    
    # Extract individual symptoms from the 'symptoms' column
    symptoms = extract_symptoms(symptoms)
    
    return symptoms


def preprocess_medication_data(medications: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """Preprocesses the medication dataset."""
    medications.columns = medications.columns.str.lower()
    medications = _standardize_column_names(medications, column_mapping)
    medications.rename(columns={"patient": "patient_id"}, inplace=True)
    return medications
