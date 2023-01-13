#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""This module contains Google Cloud Transfer operators."""
from typing import TYPE_CHECKING, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from ..hooks.CloudDataTransferServiceWithRunServiceHook import CloudDataTransferServiceWithRunServiceHook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class CloudDataTransferServiceRunJobOperator(BaseOperator):
    """
    Run a transfer operation in Google Storage Transfer Service.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CloudDataTransferServiceRunOperationOperator`

    :param job_name: (Required) The name of the transfer job.
    :param body: (Required) The request body contains data with the following structure: {"projectId": string}
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param api_version:  API version used (e.g. v1).
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    # [START gcp_transfer_operation_run_template_fields]
    template_fields: Sequence[str] = (
        'job_name',
        'gcp_conn_id',
        'api_version',
        'google_impersonation_chain',
    )
    # [END gcp_transfer_operation_run_template_fields]

    def __init__(
        self,
        *,
        job_name: str,
        body: dict,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v1",
        google_impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_name = job_name
        self.body = body
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.google_impersonation_chain = google_impersonation_chain
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        if not self.job_name:
            raise AirflowException("The required parameter 'job_name' is empty or None")
        if not self.body:
            raise AirflowException("The required parameter 'body' is empty or None")

    def execute(self, context: 'Context') -> None:
        hook = CloudDataTransferServiceWithRunServiceHook(
            api_version=self.api_version,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.google_impersonation_chain,
        )
        hook.run_transfer_operation(job_name=self.job_name, body=self.body)
