import azureml.core
import os

from azureml.core import Workspace, Datastore
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.data.data_reference import DataReference
from azureml.pipeline.core import Pipeline, PipelineData
from azureml.pipeline.steps import PythonScriptStep
from dotenv import load_dotenv


# print version of loaded libraries
print("Loaded libraries:")
print("- azureml.core, version: ", azureml.core.VERSION)


def create_workspace(subscription_id, resource_group, region, workspace_name):
    # list all workspaces
    workspaces = [workspace for workspace in Workspace.list(subscription_id, resource_group=resource_group)]

    ws = None
    if len(workspaces) == 0 or workspace_name not in workspaces:
        print('Creating a new workspace...')
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


def create_computetarget(workspace):
    compute_name = "aml-compute"
    vm_size = "STANDARD_D2_V2"

    compute_target = None
    if compute_name in ws.compute_targets:
        compute_target = ws.compute_targets[compute_name]
        if compute_target and type(compute_target) is AmlCompute:
            print('Found compute target: ' + compute_name)
    else:
        print('Creating a new compute target...')
        provisioning_config = AmlCompute.provisioning_configuration(vm_size=vm_size,
                                                                    min_nodes=0,
                                                                    max_nodes=4,
                                                                    idle_seconds_before_scaledown=600)
        # create the compute target
        compute_target = ComputeTarget.create(ws, compute_name, provisioning_config)

        # Can poll for a minimum number of nodes and for a specific timeout.
        # If no min node count is provided it will use the scale settings for the cluster
        compute_target.wait_for_completion(show_output=True, min_node_count=None, timeout_in_minutes=20)

        # For a more detailed view of current cluster status, use the 'status' property
        #print(compute_target.status.serialize())
        print("The compute target created!")
    return compute_target


def set_up_datastores(workspace, account_name, account_key, container_name):
    # List all datastores registered in the current workspace
    datastores = [datastore for datastore in workspace.datastores]

    input_datastore = None
    input_datastore_name = 'sourceblobstorage'
    if len(datastores) == 0 or input_datastore_name not in datastores:
        print("Registering input blob datastore...")
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

    # default datastore to persist interim and processed data
    output_datastore = ws.get_default_datastore()
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

    # Create an Azure Machine Learning service workspace
    print("\nSTEP 1")
    ws = create_workspace(subscription_id, resource_group, region, workspace_name)

    # Create a computer target
    print("\nSTEP 2")
    compute_target = create_computetarget(ws)

    # Create references to source/input and destination/output datasources
    print("\nSTEP 3")
    input_datastore, output_datastore = set_up_datastores(ws, storage_account_name, storage_account_key, container_name)

    input_data = DataReference(
        datastore=input_datastore,
        data_reference_name="input_data"
    )

    processed_data1 = PipelineData("processed_data1", datastore=output_datastore)

    source_directory = 'ingestion'
    print(f"Source directory for the step is {os.path.realpath(source_directory)}.")
    ingestStep = PythonScriptStep(
        script_name="ingest.py",
        arguments=["--input", input_data, "--output", processed_data1],
        inputs=[input_data],
        outputs=[processed_data1],
        compute_target=compute_target,
        source_directory=source_directory,
        allow_reuse=True)

    pipeline_steps = [ingestStep]
    pipeline1 = Pipeline(workspace=ws, steps=[pipeline_steps])

    published_pipeline1 = pipeline1.publish(name="Data Preparation Pipeline",
                                            description="Data Preparation PipelineDescription",
                                            continue_on_step_failure=True)
