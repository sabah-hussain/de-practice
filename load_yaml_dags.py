"""
This script tries to load all the YAML DAGs in the DAGs folder recursively
and ignores those that fail. Ideally you want to be much more restrictive with
the YAML DAGs you try to load. So create a better filter for the yaml files
you load if you are going to have mixed config yaml files from several
tools/modules/functionalities.
"""

# the 'DAG' and 'airflow' words must appear in the file for it to be parsed
from pathlib import Path
import logging
import yaml
from airflow.configuration import conf as airflow_conf
from dagfactory import DagFactory

COMMON_FOLDER = airflow_conf.get("core", "dags_folder")
for config_file_path in list(Path(COMMON_FOLDER).rglob('*.yaml')):
    with open(config_file_path) as config_file:
        try:
            dag_config = yaml.unsafe_load(config_file)
            DagFactory(config=dag_config).generate_dags(globals())
            logging.info(f'* DAG loaded: {config_file_path}')
        except Exception as err:
            logging.warning(f"* Failed to load {config_file_path}", err)