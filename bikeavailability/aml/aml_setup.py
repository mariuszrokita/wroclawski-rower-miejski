import azureml.core
import os

from azureml.core import Workspace, Datastore
from dotenv import load_dotenv


# print version of loaded libraries
print("Loaded libraries:")
print("- azureml.core, version: ", azureml.core.VERSION)


def create_workspace(subscription_id, resource_group, region, workspace_name):
    # list all workspaces
    workspaces = [workspace for workspace in Workspace.list(subscription_id, resource_group=resource_group)]

    ws = None
    if len(workspaces) == 0 or workspace_name not in workspaces:
        ws = Workspace.create(
            name=workspace_name,
            subscription_id=subscription_id,
            resource_group=resource_group,
            create_resource_group=True,
            location=region)
        print(f"Workspace {workspace_name} created!")
    else:
        ws = Workspace.get(name=workspace_name,
                           subscription_id=subscription_id,
                           resource_group=resource_group)
        print(f"No need to create the workspace {workspace_name}. It already exists!")
    return ws


def set_up_datastores(workspace, account_name, account_key, container_name):
    input_datastore = None
    output_datastore = None

    # default datastore to persist interim and processed data
    output_datastore = ws.get_default_datastore()

    # List all datastores registered in the current workspace
    datastores = [datastore for datastore in workspace.datastores]

    input_datastore_name = 'sourceblobstorage'
    if len(datastores) == 0 or input_datastore_name not in datastores:
        input_datastore = Datastore.register_azure_blob_container(
            workspace=workspace,
            datastore_name=input_datastore_name,
            account_name=account_name,  # Storage account name
            container_name=container_name,  # Name of Azure blob container
            account_key=account_key)  # Storage account key
        print(f"Registered input blob datastore with name: {input_datastore_name}")
    else:
        input_datastore = Datastore.get(workspace, input_datastore_name)
        print(f"No need to register input blob datastore with name: {input_datastore_name}. It's already registered!")

    return input_datastore, output_datastore


if __name__ == "__main__":
    # load environment variables
    load_dotenv()
    subscription_id = os.getenv("SUBSCRIPTION_ID")
    resource_group = os.getenv("RESOURCE_GROUP")
    region = os.getenv("REGION")
    workspace_name = os.getenv("WORKSPACE_NAME")
    storage_account_name = os.getenv("STORAGE_ACCOUNT")
    storage_account_key = os.getenv("STORAGE_KEY")
    container_name = os.getenv("CONTAINER_NAME")

    # Create Azure Machine Learning service workspace
    ws = create_workspace(subscription_id, resource_group, region, workspace_name)

    # Create references to source/input and destination/output datasources
    src_datastore, dest_datastore = set_up_datastores(ws, storage_account_name, storage_account_key, container_name)
