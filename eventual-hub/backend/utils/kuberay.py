import copy
import functools
from typing import List

import yaml
from kubernetes_asyncio import client
from models import (
    KuberayClientConfig,
    RayCluster,
    RayClusterInfo,
    RayClusterState,
    RayClusterType,
)
from settings import settings
from utils.kubernetes import k8s_retryable


@functools.lru_cache(1)
def load_config() -> KuberayClientConfig:
    with open(settings.kuberay_client_config_path) as f:
        return KuberayClientConfig.parse_obj(yaml.safe_load(f))


async def launch_ray_cluster(*, name: str, namespace: str, cluster_type: RayClusterType) -> RayCluster:
    config = load_config()
    template = copy.deepcopy(config.template)
    cluster_config = config.cluster_configs[cluster_type]

    # Set name
    template["metadata"]["name"] = name

    # Inject labels
    template["metadata"]["labels"] = {"ray.io/cluster-type": cluster_type.value}

    # Set head node resources
    head_group_container = next(
        filter(lambda d: d["name"] == "ray-head", template["spec"]["headGroupSpec"]["template"]["spec"]["containers"])
    )
    head_group_container["resources"] = {
        "limits": {"cpu": str(cluster_config.head_cpu), "memory": cluster_config.head_memory}
    }

    # Set worker max replicas
    # TODO: allow a single worker group for now?
    worker_group_spec = template["spec"]["workerGroupSpecs"][0]
    # TODO: make desired replicas and minReplicas configurable?
    # TODO: set limits based on total CPU/memory usage
    worker_group_spec["maxReplicas"] = cluster_config.max_workers
    # Set worker node resources
    worker_group_container = next(
        filter(
            lambda d: d["name"] == "ray-worker",
            worker_group_spec["template"]["spec"]["containers"],
        )
    )
    worker_group_container["resources"] = {
        "limits": {"cpu": str(cluster_config.worker_cpu), "memory": cluster_config.worker_memory}
    }

    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        data = await k8s_retryable()(api.create_namespaced_custom_object)(
            group="ray.io",
            version="v1alpha1",
            plural="rayclusters",
            namespace=namespace,
            body=template,
        )

    return RayCluster(
        name=data["metadata"]["name"],
        namespace=data["metadata"]["namespace"],
        type=data["metadata"]["labels"]["ray.io/cluster-type"],
        started_at=data["metadata"]["creationTimestamp"],
    )


async def list_ray_clusters(*, namespace: str) -> List[RayCluster]:
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        data = await k8s_retryable()(api.list_namespaced_custom_object)(
            group="ray.io", version="v1alpha1", plural="rayclusters", namespace=namespace
        )
    return [
        RayCluster(
            name=i["metadata"]["name"],
            namespace=i["metadata"]["namespace"],
            type=i["metadata"]["labels"]["ray.io/cluster-type"],
            started_at=i["metadata"]["creationTimestamp"],
        )
        for i in data["items"]
    ]


async def get_ray_cluster(*, name: str, namespace: str) -> RayClusterInfo:
    info = {}
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        cluster_data = await k8s_retryable()(api.get_namespaced_custom_object)(
            group="ray.io", version="v1alpha1", plural="rayclusters", name=name, namespace=namespace
        )

        # Check for running pods
        v1 = client.CoreV1Api(api_client=api_client)
        pod_data = await k8s_retryable()(v1.list_namespaced_pod)(
            namespace=namespace, label_selector=f"ray.io/cluster={name}", field_selector="status.phase==Running"
        )

        head_pod_running = False
        workers = 0
        for p in pod_data.items:
            if p.metadata.labels.get("ray.io/node-type") == "head":
                head_pod_running = True
            if p.metadata.labels.get("ray.io/node-type") == "worker":
                workers += 1

        if head_pod_running:
            info["state"] = RayClusterState.READY
            info["workers"] = workers

            # Check if service exists
            service_data = await k8s_retryable()(v1.list_namespaced_service)(
                namespace=namespace, label_selector=f"ray.io/cluster={name},ray.io/node-type=head"
            )
            if service_data.items:
                service = service_data.items[0]
                info["endpoint"] = f"ray://{service.metadata.name}.{service.metadata.namespace}.svc.cluster.local:10001"

    cluster = RayCluster(
        name=cluster_data["metadata"]["name"],
        namespace=cluster_data["metadata"]["namespace"],
        type=cluster_data["metadata"]["labels"]["ray.io/cluster-type"],
        started_at=cluster_data["metadata"]["creationTimestamp"],
    )
    return RayClusterInfo(cluster=cluster, **info)


async def delete_ray_cluster(*, name: str, namespace: str) -> None:
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        await k8s_retryable(ignore=[404])(api.delete_namespaced_custom_object)(
            group="ray.io", version="v1alpha1", plural="rayclusters", name=name, namespace=namespace
        )
