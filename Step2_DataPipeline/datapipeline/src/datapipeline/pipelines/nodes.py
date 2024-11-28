import pandas as pd


def _remove_digits(x: pd.Series) -> pd.Series:
    """Removes digits from strings in a Series."""
    return x.str.replace(r"\d+", "", regex=True)


def _to_lowercase(x: pd.Series) -> pd.Series:
    """Converts all strings in a Series to lowercase."""
    return x.str.lower()


def _parse_absolute(x: pd.Series) -> pd.Series:
    """Converts values in a Series to integers and then to their absolute values."""
    return x.astype(int).abs()



def preprocess_patient_data(patient_data: pd.DataFrame, gender_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the patient dataset.

    Args:
        patient_data: Raw patient dataset.
        gender_data: Dataset containing patient gender information.

    Returns:
        Preprocessed patient dataset.
    """
    patient_data.columns = patient_data.columns.str.lower()
    patient_data["first"] = _remove_digits(patient_data["first"])
    patient_data["maiden"] = _remove_digits(patient_data["maiden"])
    patient_data["last"] = _remove_digits(patient_data["last"])
    patient_data["income"] = _parse_absolute(patient_data["income"])
    patient_data = patient_data.merge(gender_data, left_on="patient_id",right_on="Id" ,how="left")
    return patient_data


def preprocess_conditions_data(conditions: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the conditions dataset.

    Args:
        conditions: Raw conditions dataset.

    Returns:
        Preprocessed conditions dataset.
    """
    conditions.columns = conditions.columns.str.lower()
    conditions["description"] = _to_lowercase(conditions["description"])
    conditions.rename(
        columns={
            "start": "start_date",
            "end": "end_date",
            "code": "condition_code",
            "patient": "patient_id",
            "encounter": "encounter_id",
        },
        inplace=True,
    )
    return conditions


def preprocess_encounters_data(encounters: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the encounters dataset.

    Args:
        encounters: Raw encounters dataset.

    Returns:
        Preprocessed encounters dataset.
    """
    encounters.columns = encounters.columns.str.lower()
    encounters.rename(
        columns={
            "id": "encounter_id",
            "start": "start_datetime",
            "end": "end_datetime",
            "patient": "patient_id",
            "code": "encounter_code",
        },
        inplace=True,
    )
    return encounters


def preprocess_symptoms_data(symptoms: pd.DataFrame, gender_data: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the symptoms dataset.

    Args:
        symptoms: Raw symptoms dataset.
        gender_data: Dataset containing patient gender information.

    Returns:
        Preprocessed symptoms dataset.
    """
    symptoms.columns = symptoms.columns.str.lower()
    symptoms.rename(columns={"patient": "patient_id"}, inplace=True)
    symptoms = symptoms.merge(gender_data,left_on="patient_id",right_on="Id", how="left")
    return symptoms


def preprocess_medication_data(medications: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the medication dataset.

    Args:
        medications: Raw medication dataset.

    Returns:
        Preprocessed medication dataset.
    """
    medications.columns = medications.columns.str.lower()
    medications.rename(
        columns={
            "start": "start_datetime",
            "end": "end_datetime",
            "patient": "patient_id",
            "encounter": "encounter_id",
            "code": "medication_code",
        },
        inplace=True,
    )
    return medications
