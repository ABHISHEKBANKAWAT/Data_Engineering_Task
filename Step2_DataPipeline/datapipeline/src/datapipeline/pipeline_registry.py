"""Project pipelines."""

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline
from datapipeline.pipelines.pipeline import create_pipeline  # Replace <project_name> with your project name


def register_pipelines() -> dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    # Dynamically find and include pipelines defined elsewhere in the project
    pipelines = find_pipelines()

    # Add your custom pipeline
    pipelines["cleaning_pipeline"] = create_pipeline()

    # Define the default pipeline
    pipelines["__default__"] = sum(pipelines.values())
    
    return pipelines
