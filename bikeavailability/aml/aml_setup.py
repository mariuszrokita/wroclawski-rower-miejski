import azureml.core
import os

from azureml.core import Workspace, Datastore
from dotenv import load_dotenv


# print version of loaded libraries
print("Loaded libraries:")
print("azureml.core, version: ", azureml.core.VERSION)


def create_workspace(subscription_id, resource_group, region, workspace_name):
    # list all workspaces
    workspaces = [workspace for workspace in Workspace.list(subscription_id, resource_group=resource_group)]

    ws = None
    if len(workspaces) == 0 or workspace_name not in workspaces:
        ws = Workspace.create(name=workspace_name,
                              subscription_id=subscription_id,
                              resource_group=resource_group,
                              create_resource_group=True,
                              location=region)
        print(f"Workspace {workspace_name} created!")
    else:
        ws = Workspace.get(name=workspace_name,
                           subscription_id=subscription_id,
                           resource_group=resource_group)
        print(f"Workspace {workspace_name} already exists!")
    return ws


if __name__ == "__main__":
    # load environment variables
    load_dotenv()
    subscription_id = os.getenv("SUBSCRIPTION_ID")
    resource_group = os.getenv("RESOURCE_GROUP")
    region = os.getenv("REGION")
    workspace_name = os.getenv("WORKSPACE_NAME")

    ws = create_workspace(subscription_id, resource_group, region, workspace_name)
    print(ws.get_details())
