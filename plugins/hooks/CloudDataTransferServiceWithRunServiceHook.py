from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    CloudDataTransferServiceHook,
)

from typing import Optional, Sequence, Union

class CloudDataTransferServiceWithRunServiceHook(CloudDataTransferServiceHook):
    """
    Hook for Google Storage Transfer Service.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        api_version: str = 'v1',
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version
        self._conn = None
        
    def run_transfer_operation(self, job_name: str, body: dict) -> None:
        """
        Runs an transfer operation in Google Storage Transfer Service.

        :param job_name: Name of the transfer operation.
        :param body: Body of the transfer operation
        :rtype: None
        """
        self.get_conn().transferJobs().run(jobName=job_name, body=body).execute(num_retries=self.num_retries)
