# Implement function with sdk that runs find the pipeline in databricks and starts it and waits for it to complete
import os
import datetime
from behave import given, when, then  # type: ignore
from src.framework.helper import get_logger
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


# Initialize logger
logger = get_logger(__name__)
workspace_client = WorkspaceClient()
TIMEOUT = 3600  # seconds


def _get_databricks_job_by_name(workspace_client: WorkspaceClient, job_name: str):
    return next(
        filter(
            lambda job: job.settings.name.startswith(job_name)
            and (
                "environment" not in job.settings.tags
                or job.settings.tags["environment"]
                == os.getenv("DATABRICKS_ENVIRONMENT", "dev")
            ),
            workspace_client.jobs.list(),
        )
    )


@given("that the databricks job exists")  # type : ignore
def step_impl(context, job_name="lakehouse_demo_project_job"):
    job = _get_databricks_job_by_name(workspace_client, job_name)
    context.job_id = job.job_id
    context.api = jobs.JobsAPI(workspace_client.api_client)


@when("the job is triggered")
def step_impl(context):
    try:
        context.run = context.api.run_now_and_wait(
            job_id=context.job_id, timeout=datetime.timedelta(seconds=TIMEOUT)
        )

    except Exception as e:
        logger.error(f"Error monitoring job: {e}")
        raise


@then("the job should complete successfully")
def step_impl(context):
    if context.run.state.life_cycle_state != "TERMINATED":
        raise Exception(
            f"Job did not terminate properly. Current state: {context.run.state.life_cycle_state}"
        )

    if context.run.state.result_state != "SUCCESS":
        raise Exception(
            f"Job did not complete successfully. Result state: {context.run.state.result_state}"
        )

    logger.info(f"Job completed successfully with run ID: {context.run.run_id}")
