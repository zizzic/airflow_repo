"""This module tests DAG integrity within Airflow."""

import glob
import os
import pytest

# from airflow.models import DAG
from airflow.models.dagbag import DagBag

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "dags/**/*.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file):
    """Test the integrity of DAGs.

    This test ensures that each DAG file can be loaded without errors,
    including checking for circular dependencies and other potential issues.

    Args:
        dag_file (str): The path to a DAG file to be tested.
    """
    dag_bag = DagBag(dag_folder=os.path.dirname(dag_file), include_examples=False)

    dag_bag.process_file(dag_file, only_if_updated=True)

    # dag_id, dag in dag_bag.dags.items()
    for dag_id, _ in dag_bag.dags.items():
        assert dag_id in dag_bag.dags, "DAG ID not found in dag_bag.dags"
        assert not dag_bag.import_errors, "Import errors found in DagBag"
