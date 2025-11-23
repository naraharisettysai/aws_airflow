from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

class PushDagRunConfigOperator(BaseOperator):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context: Context):
        # Get dag_run.conf if exists
        conf = context.get("dag_run").conf if context.get("dag_run") else {}
        print(conf)
        for key,value in conf.items():
            context["ti"].xcom_push(key=key, value=value)
            
        self.log.info(f"Pushing dag_run.conf to XCom: {conf}")
        return conf  # XCom push happens automatically when you return something