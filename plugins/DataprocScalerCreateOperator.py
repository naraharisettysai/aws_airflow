# custom_operators/dataproc_scaler_operator.py

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import dataproc_v1

class DataprocScalerCreateOperator(BaseOperator):
    """
    Custom operator to create a Dataproc autoscaling policy.
    """

    @apply_defaults
    def __init__(
        self,
        project_id,
        region,
        policy_id,
        policy_config,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.policy_id = policy_id
        self.policy_config = policy_config

    def execute(self, context):
        client = dataproc_v1.AutoscalingPolicyServiceClient()
        parent = f"projects/{self.project_id}/regions/{self.region}"

        policy = dataproc_v1.AutoscalingPolicy(
            id=self.policy_id,
            basic_algorithm=self.policy_config['basic_algorithm'],
            worker_config=self.policy_config['worker_config'],
            secondary_worker_config=self.policy_config.get('secondary_worker_config', None),
        )

        request = dataproc_v1.CreateAutoscalingPolicyRequest(
            parent=parent,
            policy=policy,
        )

        response = client.create_autoscaling_policy(request=request)
        self.log.info(f"Created Dataproc autoscaling policy: {response.name}")
        return response.name