import azureml.core
import os

from azureml.core import Datastore, Experiment, Workspace
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


def create_compute_target(workspace):
    compute_name = "aml-compute"
    vm_size = "Standard_D1_v2"

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
                                                                    idle_seconds_before_scaledown=900)
        # create the compute target
        compute_target = ComputeTarget.create(ws, compute_name, provisioning_config)

        # Can poll for a minimum number of nodes and for a specific timeout.
        # If no min node count is provided it will use the scale settings for the cluster
        compute_target.wait_for_completion(show_output=True, min_node_count=None, timeout_in_minutes=20)

        # For a more detailed view of current cluster status, use the 'status' property
        # print(compute_target.status.serialize())
        print("The compute target created!")
    return compute_target


def set_up_input_datastore(workspace, account_name, account_key, container_name):
    # List all datastores registered in the current workspace
    datastores = [datastore for datastore in workspace.datastores]

    input_datastore = None
    input_datastore_name = 'sourceblobstorage'
    if len(datastores) == 0 or input_datastore_name not in datastores:
        print("Registering input blob datastore...")
        input_datastore = Datastore.register_azure_blob_container(
            workspace=workspace,
            datastore_name=input_datastore_name,
            account_name=account_name,
            container_name=container_name,
            account_key=account_key)
        print(f"Registered input blob datastore with name: {input_datastore_name}")
    else:
        input_datastore = Datastore.get(workspace, input_datastore_name)
        print(f"No need to register input blob datastore with name: {input_datastore_name}. It's already registered!")
    return input_datastore


def set_up_output_datastore(workspace, account_name, account_key, container_name):
    # List all datastores registered in the current workspace
    datastores = [datastore for datastore in workspace.datastores]

    output_datastore = None
    output_datastore_name = 'outputblobstorage'
    if len(datastores) == 0 or output_datastore_name not in datastores:
        print("Registering output blob datastore...")
        output_datastore = Datastore.register_azure_blob_container(
            workspace=workspace,
            datastore_name=output_datastore_name,
            account_name=account_name,
            container_name=container_name,
            account_key=account_key)
        print(f"Registered output blob datastore with name: {output_datastore_name}")
    else:
        output_datastore = Datastore.get(workspace, output_datastore_name)
        print(f"No need to register input blob datastore with name: {output_datastore_name}. It's already registered!")
    return output_datastore


def create_step_ingest_and_convert(input_data_loc, intermediate_data_loc, output_data, compute_target):
    source_directory = 'ingestion'
    print(f"Source directory for the step is {os.path.realpath(source_directory)}.")
    # TODO: The converted csv file should be copied to a known 'fix' location.
    step = PythonScriptStep(
        name="Ingest json files and convert to csv",
        script_name="ingest.py",
        arguments=["--input", input_data_loc,
                   "--output", output_data,
                   "--converted_data_location", intermediate_data_loc],
        inputs=[input_data_loc, intermediate_data_loc],
        outputs=[output_data],
        compute_target=compute_target,
        source_directory=source_directory,
        allow_reuse=True)
    print("Step created!")
    return step


if __name__ == "__main__":
    # load environment variables
    load_dotenv()
    subscription_id = os.getenv("SUBSCRIPTION_ID")
    resource_group = os.getenv("RESOURCE_GROUP")
    region = os.getenv("REGION")
    workspace_name = os.getenv("WORKSPACE_NAME")
    input_storage_account_name = os.getenv("INPUT_STORAGE_ACCOUNT")
    input_storage_account_key = os.getenv("INPUT_STORAGE_KEY")
    input_container_name = os.getenv("INPUT_CONTAINER_NAME")
    output_storage_account_name = os.getenv("OUTPUT_STORAGE_ACCOUNT")
    output_storage_account_key = os.getenv("OUTPUT_STORAGE_KEY")
    output_container_name = os.getenv("OUTPUT_CONTAINER_NAME")

    # Create an Azure Machine Learning service workspace
    print("\nSTEP 1")
    ws = create_workspace(subscription_id, resource_group, region, workspace_name)

    # Create a computer target
    print("\nSTEP 2")
    compute_target = create_compute_target(ws)

    # Create source/input datasource that contains raw json files
    print("\nSTEP 3")
    input_datastore = set_up_input_datastore(
        workspace=ws,
        account_name=input_storage_account_name,
        account_key=input_storage_account_key,
        container_name=input_container_name)

    print("Creating reference to raw input data...")
    input_data_ref = DataReference(
        datastore=input_datastore,
        data_reference_name="raw_data")
    print("done!")

    # Datastore that should store data available to all pipeline steps
    print("\nSTEP 4")
    output_datastore = set_up_output_datastore(
        workspace=ws,
        account_name=output_storage_account_name,
        account_key=output_storage_account_key,
        container_name=output_container_name)

    converted_data_ref = DataReference(
        datastore=output_datastore,
        data_reference_name="converted_data")

    # Default datastore to exchange data between pipeline steps
    print("\nSTEP 5")
    print("Creating datastore used to exchange data between pipeline steps")
    pipeline_datastore = ws.get_default_datastore()

    print("Creating pipeline data object (for data exchanged between pipeline steps)")
    pipeline_data = PipelineData("pipeline_data", datastore=pipeline_datastore)

    # Create pipeline steps
    print("\nSTEP 6")
    ingest_step = create_step_ingest_and_convert(
        input_data_loc=input_data_ref,
        intermediate_data_loc=converted_data_ref,
        output_data=pipeline_data,
        compute_target=compute_target)

    # finally, create pipeline and publish it
    # TODO: Is there any way to 'update' existing pipeline, instead of creating a new one?
    print("\nSTEP 7")
    print("Creating pipeline...")
    pipeline_steps = [ingest_step]
    pipeline = Pipeline(workspace=ws, steps=[pipeline_steps])
    print("done!")

    # TODO: Let the data preparation pipeline be executed in the scheduled manner
    print("\nSTEP 8")
    print("Publishing pipeline...")
    published_pipeline = pipeline.publish(
        name="Data Preparation Pipeline",
        description="Data Preparation PipelineDescription",
        continue_on_step_failure=True)
    print("done!")

    print("\nSTEP 9")
    print("Submitting pipeline...")
    experiment = Experiment(ws, 'Data_Preparation_Pipeline')
    pipeline_run = experiment.submit(published_pipeline, regenerate_outputs=False)
