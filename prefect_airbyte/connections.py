"""Tasks for connecting to Airbyte and triggering connection syncs"""
import uuid
from asyncio import sleep

from prefect import flow, get_run_logger
from prefect.blocks.core import Block
from prefect.exceptions import MissingContextError
from prefect.logging import get_logger
from pydantic import BaseModel, Field
from typing_extensions import Literal

from prefect_airbyte import exceptions as err
from prefect_airbyte.server import AirbyteServer

# Connection statuses
CONNECTION_STATUS_ACTIVE = "active"
CONNECTION_STATUS_INACTIVE = "inactive"
CONNECTION_STATUS_DEPRECATED = "deprecated"

# Job statuses
JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_FAILED = "failed"
JOB_STATUS_PENDING = "pending"


class AirbyteSyncJobResult(BaseModel):
    """Model representing a result from an Airbyte sync job."""

    created_at: int
    job_status: Literal["succeeded", "failed", "pending"]
    job_id: uuid.UUID
    updated_at: int


class AirbyteSync(Block):
    @property
    def logger(self):
        try:
            return get_run_logger()
        except MissingContextError:
            return get_logger()

    airbyte_server: AirbyteServer = Field(
        default=...,
    )

    timeout: int = Field(
        default=5,
        description="Timeout in seconds for requests made by `httpx.AsyncClient`.",
    )

    async def trigger(
        self,
        connection_id: str,
    ):

        try:
            uuid.UUID(connection_id)
        except (TypeError, ValueError):
            raise ValueError(
                "Parameter `connection_id` *must* be a valid UUID \
                i.e. 32 hex characters, including hyphens."
            )

        async with self.airbyte_server.get_client(
            logger=self.logger, timeout=self.timeout
        ) as airbyte_client:

            self.logger.info(
                f"Triggering Airbyte Connection {connection_id}, "
                f"in workspace at `airbyte_base_url` {self.airbyte_server.base_url}"
            )

            connection_status = await airbyte_client.get_connection_status(
                connection_id
            )

            if connection_status == CONNECTION_STATUS_ACTIVE:
                # Trigger manual sync on the Connection ...
                (
                    job_id,
                    _,
                ) = await airbyte_client.trigger_manual_sync_connection(connection_id)

                return job_id

            elif connection_status == CONNECTION_STATUS_INACTIVE:
                self.logger.error(
                    f"Connection: {connection_id} is inactive"
                    " - you'll need to enable it in your Airbyte instance"
                )
                raise err.AirbyteConnectionInactiveException(
                    f"Please enable the Connection {connection_id} in Airbyte instance."
                )
            elif connection_status == CONNECTION_STATUS_DEPRECATED:
                self.logger.error(f"Connection {connection_id} is deprecated.")
                raise err.AirbyeConnectionDeprecatedException(
                    f"Connection {connection_id} is deprecated."
                )

    async def wait_for_completion(
        self,
        job_id: str,
        poll_interval_s: int = 15,
        status_updates: bool = False,
    ):
        with self.airbyte_server.get_client(
            logger=self.logger, timeout=self.timeout
        ) as airbyte_client:

            job_status = await airbyte_client.get_job_status(self.job_id)

            while job_status not in [JOB_STATUS_FAILED, JOB_STATUS_SUCCEEDED]:
                (
                    job_status,
                    _,
                    _,
                ) = await airbyte_client.get_job_status(job_id)

                # pending┃running┃incomplete┃failed┃succeeded┃cancelled
                if job_status == JOB_STATUS_SUCCEEDED:
                    self.logger.info(f"Job {job_id} succeeded.")
                elif job_status == JOB_STATUS_FAILED:
                    self.logger.error(f"Job {job_id} failed.")
                    raise err.AirbyteSyncJobFailed(f"Job {job_id} failed.")
                else:
                    if status_updates:
                        self.logger.info(job_status)
                    # wait for next poll interval
                    await sleep(poll_interval_s)

    async def fetch_results(self, job_id: uuid.UUID) -> AirbyteSyncJobResult:
        with self.airbyte_server.get_client(
            logger=self.logger, timeout=self.timeout
        ) as airbyte_client:
            (
                job_status,
                job_created_at,
                job_updated_at,
            ) = await airbyte_client.get_job_status(job_id)

        return AirbyteSyncJobResult(
            created_at=job_created_at,
            job_id=job_id,
            job_status=job_status,
            updated_at=job_updated_at,
        )


@flow
def trigger_sync_and_wait_for_completion(
    connection_id: str,
    airbyte_sync_job_block: AirbyteSync,
    poll_interval_s: int = 15,
    status_updates: bool = False,
) -> AirbyteSyncJobResult:
    job_id = airbyte_sync_job_block.trigger(connection_id=connection_id)

    airbyte_sync_job_block.wait_for_completion(
        job_id=job_id,
        poll_interval_s=poll_interval_s,
        status_updates=status_updates,
    )

    return airbyte_sync_job_block.fetch_results(job_id=job_id)
