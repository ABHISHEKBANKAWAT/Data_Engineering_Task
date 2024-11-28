from kedro.pipeline import Pipeline, node, pipeline
import nodes
from nodes import preprocess_raw_patient_data,preprocess_raw_conditions_data, preprocess_raw_encounters_data, preprocess_raw_symptoms_data, preprocess_raw_medications_data



def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_raw_patient_data,
                inputs=["raw_patient_data", "raw_patient_gender_data"],
                outputs="cleaned_patient_data",
                name="preprocess_raw_patient_data_node",
            ),
            node(
                func=preprocess_raw_conditions_data,
                inputs=["raw_conditions_data", "raw_patient_gender_data"],
                outputs="cleaned_conditions_data",
                name="preprocess_raw_conditions_data_node",
            ),
            node(
                func=preprocess_raw_encounters_data,
                inputs="raw_encounters_data",
                outputs="cleaned_encounters_data",
                name="preprocess_raw_encounters_data_node",
            ),
            node(
                func=preprocess_raw_symptoms_data,
                inputs=["raw_symptoms_data", "raw_patient_gender_data"],
                outputs="cleaned_symptoms_data",
                name="preprocess_raw_symptoms_data_node",
            ),
            node(
                func=preprocess_raw_medications_data,
                inputs="raw_medications_data",
                outputs="cleaned_medications_data",
                name="preprocess_raw_medications_data_node",
            ),
        ]
    )
