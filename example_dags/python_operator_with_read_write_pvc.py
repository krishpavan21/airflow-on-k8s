
import logging
import os
from datetime import datetime, timedelta
 
from airflow import DAG
from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python import PythonOperator
from airflow.settings import AIRFLOW_HOME
 
log = logging.getLogger(__name__)
 
try:
    from kubernetes.client import models as k8s
 
    resources = k8s.V1ResourceRequirements(
                    limits={"cpu": "2", "memory": "2000Mi"}
                )
 
    with DAG(
        dag_id='dag_with_read_write_pvc',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['pvc_example'],
    ) as dag:
 
        # You can use annotations on your kubernetes pods!
        start_task = PythonOperator(
            task_id="start_task",
            python_callable=print_stuff,
            execution_timeout=timedelta(minutes=15),
            executor_config={
                "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
            },
        )
 
        #[START volume_write_task]
        volume_write_task = PythonOperator(
            task_id="volume_write_task",
            python_callable=print_stuff,
            execution_timeout=timedelta(minutes=15),
            executor_config={
                "pod_override": k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                            ),
                            k8s.V1Container(
                                name="sidecar",
                                image="gl-mgmt-pulp-vm1.gl-hpe.net/busybox:latest",
                                resources=resources,
                                command=["sh", "-cx"],
                                args=["echo 'This is a text written by write pod' > /mydrive/mydata.txt"],
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        mount_path="/mydrive/", name="airflow-workflow-pvc", sub_path=None, read_only=False
                                    )
                                ],
                            )
                        ],
                        volumes=[
                            k8s.V1Volume(
                                name="airflow-workflow-pvc",
                                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-workflow-pvc")
                            )
                        ],
                    )
                ),
            },
        )
        # [END volume_write_task]
 
        #[START volume_read_task]
        volume_read_task = PythonOperator(
            task_id="volume_read_task",
            python_callable=print_stuff,
            execution_timeout=timedelta(minutes=15),
            executor_config={
                "pod_override": k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                            ),
                            k8s.V1Container(
                                name="sidecar",
                                image="ci-gl-mgmt-pulp-vm1.glhc-hpe.local/busybox:latest",
                                resources=resources,
                                command=["cat"],
                                args=["/foo/mydata.txt"],
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        mount_path="/foo/", name="airflow-workflow-pvc", sub_path=None, read_only=False
                                    )
                                ],
                            )
                        ],
                        volumes=[
                            k8s.V1Volume(
                                name="airflow-workflow-pvc",
                                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-workflow-pvc")
                            )
                        ],
                    )
                ),
            },
        )
        # [END volume_read_task]
 
        start_task >> volume_write_task >> volume_read_task
except ImportError as e:
    log.warning("Could not import DAGs in example_kubernetes_executor_config.py: %s", str(e))
    log.warning("Install kubernetes dependencies with: pip install apache-airflow['cncf.kubernetes']")
