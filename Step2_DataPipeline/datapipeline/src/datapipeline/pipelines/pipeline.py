from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    preprocess_patient_data,
    preprocess_conditions_data,
    preprocess_encounters_data,
    preprocess_symptoms_data,
    preprocess_medication_data,
    create_master_database
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_patient_data,
                inputs=["raw_patient_data", "raw_patient_gender_data", "params:patient_column_mapping"],
                outputs="clean_patient_data",
                name="preprocess_patient_data_node",
            ),
            node(
                func=preprocess_conditions_data,
                inputs=["raw_conditions_data", "params:conditions_column_mapping"],
                outputs="clean_conditions_data",
                name="preprocess_conditions_data_node",
            ),
            node(
                func=preprocess_encounters_data,
                inputs=["raw_encounters_data", "params:encounters_column_mapping"],
                outputs="clean_encounters_data",
                name="preprocess_encounters_data_node",
            ),
            node(
                func=preprocess_symptoms_data,
                inputs=["raw_symptoms_data", "raw_patient_gender_data", "params:symptoms_column_mapping"],
                outputs="clean_symptoms_data",
                name="preprocess_symptoms_data_node",
            ),
            node(
                func=preprocess_medication_data,
                inputs=["raw_medications_data", "params:medications_column_mapping"],
                outputs="clean_medications_data",
                name="preprocess_medication_data_node",
            ),
            node(
                func=create_master_database,
                inputs=[
                    "clean_patient_data",
                    "clean_conditions_data",
                    "clean_encounters_data",
                    "clean_symptoms_data",
                    "clean_medications_data",
                ],
                outputs=None,  # No in-memory output since the function saves to a file
                name="create_master_database_node",
            ),
        ]
    )
