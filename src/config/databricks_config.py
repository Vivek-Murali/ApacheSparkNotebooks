"""
Configuration settings for Databricks connection and workspace.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Databricks workspace configuration
DATABRICKS_HOST = os.getenv('DATABRICKS_HOST')
DATABRICKS_TOKEN = os.getenv('DATABRICKS_TOKEN')
DATABRICKS_WORKSPACE_ID = os.getenv('DATABRICKS_WORKSPACE_ID')

# Cluster configuration
CLUSTER_CONFIG = {
    'spark_version': '13.3.x-scala2.12',
    'node_type_id': 'Standard_DS3_v2',
    'driver_node_type_id': 'Standard_DS3_v2',
    'min_workers': 1,
    'max_workers': 2,
    'autotermination_minutes': 60
}

# Storage configuration
STORAGE_CONFIG = {
    'delta_lake_path': '/dbfs/mnt/delta',
    'checkpoint_location': '/dbfs/mnt/checkpoints'
}

# MLflow configuration
MLFLOW_CONFIG = {
    'experiment_name': 'portfolio_project',
    'model_registry_name': 'portfolio_models'
} 

EVENT_REGISTRY_API_KEY = os.getenv('EVENT_REGISTRY')
EVENT_TOKEN = os.getenv('EVENT_TOKEN')