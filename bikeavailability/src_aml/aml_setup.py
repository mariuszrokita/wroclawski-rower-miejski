import azureml.core
import os

from azureml.core import Datastore, Workspace
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.data.data_reference import DataReference
from azureml.pipeline.core import Pipeline, PipelineData, PublishedPipeline
from azureml.pipeline.core.schedule import Schedule, ScheduleRecurrence
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
        provisioning_config = AmlCompute.provisioning_configuration(
            vm_size=vm_size,
            min_nodes=0,
            max_nodes=4,
            idle_seconds_before_scaledown=60)
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
    step = PythonScriptStep(
        name="Ingest json files and convert to csv",
        script_name="ingest.py",
        arguments=["--input_data_loc", input_data_loc,
                   "--output_data_loc", output_data,
                   "--intermediate_data_loc", intermediate_data_loc],
        # The intermediate_data_loc parameter indicates a location that in fact in the output location.
        # The result of a script execution will be saved there. However, it had to be declared as
        # input parameter, because a type of this parameter (DataReference) cannot be accepted as output
        # by the PythonScriptStep class (have a look at a documentation for the PythonScriptStep class).
        inputs=[input_data_loc, intermediate_data_loc],
        outputs=[output_data],
        compute_target=compute_target,
        source_directory=source_directory,
        allow_reuse=False  # process new raw data files every time the step is executed
    )
    print("Step created!")
    return step


def create_step_clean(input_data_loc, output_data_loc, compute_target):
    source_directory = "cleaning"
    print(f"Source directory for the step is {os.path.realpath(source_directory)}.")
    step = PythonScriptStep(
        name="Data cleaning",
        script_name="clean.py",
        arguments=["--input_cleaning", input_data_loc,
                   "--output_cleaning", output_data_loc],
        inputs=[input_data_loc],
        outputs=[output_data_loc],
        compute_target=compute_target,
        source_directory=source_directory,
        allow_reuse=False
    )
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
    ws.write_config()

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
        data_reference_name="INPUT_bikeavailability_raw_json_data")
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
        data_reference_name="OUTPUT_bikeavailability_data_converted_to_csv")

    # Default datastore to exchange data between pipeline steps
    print("\nSTEP 5")
    print("Creating a datastore used to exchange data between pipeline steps")
    pipeline_datastore = ws.get_default_datastore()

    print("Creating a pipeline data object (for data exchanged between pipeline steps)")
    ingestion_output_pipeline_data = PipelineData(
        name="ingestion_output_pipeline_data",
        datastore=pipeline_datastore,
        is_directory=False)  # single file
    cleaning_output_pipeline_data = PipelineData(
        name="cleaning_output_pipeline_data",
        datastore=pipeline_datastore,
        is_directory=False)  # singe file

    # Create pipeline steps
    print("\nSTEP 6")
    step_ingest = create_step_ingest_and_convert(
        input_data_loc=input_data_ref,
        intermediate_data_loc=converted_data_ref,
        output_data=ingestion_output_pipeline_data,
        compute_target=compute_target)

    step_cleaning = create_step_clean(
        input_data_loc=ingestion_output_pipeline_data,
        output_data_loc=cleaning_output_pipeline_data,
        compute_target=compute_target)

    # Finally, create pipeline and publish it
    # TODO: Is there any way to 'update' existing pipeline, instead of creating a new one?
    print("\nSTEP 7")
    print("Creating a pipeline...")
    pipeline_steps = [step_ingest, step_cleaning]
    pipeline = Pipeline(workspace=ws, steps=[pipeline_steps])
    print("done!")

    print("\nSTEP 8")
    print("Deactivation of all previous pipelines...")
    for schedule in Schedule.list(workspace=ws):
        schedule.disable()

    for published_pipeline in PublishedPipeline.list(workspace=ws):
        published_pipeline.disable()
    print("done!")

    print("\nSTEP 9")
    print("Publishing the pipeline...")
    published_pipeline = pipeline.publish(
        name="Data Preparation Pipeline",
        description="Data Preparation PipelineDescription",
        continue_on_step_failure=False)
    print("done!")

    print("\nSTEP 10")
    print("Creating a schedule for the pipeline...")
    # run every 6 hours
    recurrence = ScheduleRecurrence(frequency="Hour", interval=6)
    recurring_schedule = Schedule.create(
        ws,
        name="MyRecurringSchedule",
        description="Based on time",
        pipeline_id=published_pipeline.id,
        experiment_name='Data_Preparation_Pipeline',
        recurrence=recurrence)
    print("done!")
