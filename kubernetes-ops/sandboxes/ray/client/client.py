import asyncio
import copy
import datetime
import enum
import functools
import json
from typing import (Any, Awaitable, Callable, Dict, List, Optional, TypeVar,
                    overload)

import pydantic
import tenacity
import yaml
from kubernetes_asyncio import client, config

T = TypeVar("T")
FuncT = Callable[..., Awaitable[T]]
FuncOptT = Callable[..., Awaitable[Optional[T]]]


def _should_retry(e: Exception) -> bool:
    # Retry on transient timeouts
    if isinstance(e, (TimeoutError, asyncio.TimeoutError)):
        return True

    if not isinstance(e, client.rest.ApiException):
        return False

    # Too many requests
    if e.status == 429:
        return True

    if e.status == 500:
        try:
            body_dict = json.loads(e.body)
        except json.decoder.JSONDecodeError:
            return False

        # ServerTimeout
        if body_dict["reason"] == "ServerTimeout":
            return True

        return False

    return e.status > 500


@overload
def retryable() -> Callable[[FuncT], FuncT]:
    pass


@overload
def retryable(ignore: List[int]) -> Callable[[FuncT], FuncOptT]:
    pass


def retryable(ignore: Optional[List[int]] = None) -> Callable[[FuncT], FuncOptT]:
    def decorator(func: FuncT) -> FuncOptT:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            try:
                # TODO: tune retry policy
                retry_decorator = tenacity.retry(
                    stop=tenacity.stop_after_attempt(10),
                    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10) + tenacity.wait_random(0, 2),
                    retry=tenacity.retry_if_exception(_should_retry),
                    reraise=True,
                )

                # `func` maybe an asyncio coroutine function or a callable
                # that returns an asyncio coroutine. In order to seamlessly
                # handle both these cases, we wrap it as follows:
                @retry_decorator
                async def _inner() -> Optional[T]:
                    return await func(*args, **kwargs)

                return await _inner()

            except client.rest.ApiException as e:
                if ignore is None or e.status not in ignore:
                    raise
                return None

        return wrapper

    return decorator


class ClusterType(enum.Enum):
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class ClusterConfig(pydantic.BaseModel):
    head_cpu: int
    head_memory: str
    worker_cpu: int
    worker_memory: str
    max_workers: int


class ClientConfig(pydantic.BaseModel):
    cluster_configs: Dict[ClusterType, ClusterConfig]
    template: Dict[str, Any]


# TODO: hot reload from k8s configmap?
@functools.lru_cache(1)
def load_config() -> ClientConfig:
    with open("config.yaml") as f:
        return ClientConfig.parse_obj(yaml.safe_load(f))


class RayClusterState(enum.Enum):
    PENDING = "pending"
    READY = "ready"


class RayClusterEvent(pydantic.BaseModel):
    timestamp: datetime.datetime
    note: str
    reason: str
    type: str


class RayCluster(pydantic.BaseModel):
    name: str
    namespace: str
    type: ClusterType


class RayClusterInfo(pydantic.BaseModel):
    cluster: RayCluster
    state: RayClusterState = RayClusterState.PENDING
    endpoint: Optional[str] = None
    workers: int = 0


# TODO: what else do we want to parametrize?
# TODO: switch to t-shirt size node configurations!!
async def create_ray_cluster(*, name: str, namespace: str, cluster_type: ClusterType) -> RayCluster:
    config = load_config()
    template = copy.deepcopy(config.template)
    cluster_config = config.cluster_configs[cluster_type]

    # Set name
    template["metadata"]["name"] = name

    # Inject labels
    template["metadata"]["labels"] = {"evntl.io/ray-cluster-type": cluster_type.value}

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
        await retryable()(api.create_namespaced_custom_object)(
            group="ray.io",
            version="v1alpha1",
            plural="rayclusters",
            namespace=namespace,
            body=template,
        )

    return RayCluster(name=name, namespace=namespace, type=cluster_type)


async def delete_ray_cluster(*, name: str, namespace: str) -> None:
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        await retryable(ignore=[404])(api.delete_namespaced_custom_object)(
            group="ray.io", version="v1alpha1", plural="rayclusters", name=name, namespace=namespace
        )


async def list_ray_clusters(*, namespace: str) -> List[RayCluster]:
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        data = await retryable()(api.list_namespaced_custom_object)(
            group="ray.io", version="v1alpha1", plural="rayclusters", namespace=namespace
        )
    return [
        RayCluster(
            name=i["metadata"]["name"],
            namespace=i["metadata"]["namespace"],
            type=i["metadata"]["labels"]["evntl.io/ray-cluster-type"],
        )
        for i in data["items"]
    ]


async def _get_events_for_object(*, uid: str, namespace: str) -> List[RayClusterEvent]:
    class FakeEventTime:
        def __get__(self, obj, objtype=None):
            return obj._event_time

        def __set__(self, obj, value):
            obj._event_time = value

    # Monkey-patch the `event_time` attribute of ` the V1beta1Event class.
    # See: https://github.com/kubernetes-client/python/issues/1826
    # Fix from: https://stackoverflow.com/a/72591958
    client.V1beta1Event.event_time = FakeEventTime()

    async with client.ApiClient() as api_client:
        api = client.EventsV1beta1Api(api_client=api_client)
        data = await retryable()(api.list_namespaced_event)(namespace="default", field_selector=f"regarding.uid={uid}")
    return [
        RayClusterEvent(
            timestamp=i.metadata.creation_timestamp.isoformat(),
            **{k: getattr(i, k) for k in ("note", "reason", "type")},
        )
        for i in data.items
    ]


async def describe_ray_cluster(*, name: str, namespace: str) -> RayClusterInfo:
    info = {}
    async with client.ApiClient() as api_client:
        api = client.CustomObjectsApi(api_client=api_client)
        cluster_data = await retryable()(api.get_namespaced_custom_object)(
            group="ray.io", version="v1alpha1", plural="rayclusters", name=name, namespace=namespace
        )

        # Check for running pods
        v1 = client.CoreV1Api(api_client=api_client)
        pod_data = await retryable()(v1.list_namespaced_pod)(
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
            service_data = await retryable()(v1.list_namespaced_service)(
                namespace=namespace, label_selector=f"ray.io/cluster={name},ray.io/node-type=head"
            )
            if service_data.items:
                service = service_data.items[0]
                info["endpoint"] = f"ray://{service.metadata.name}.{service.metadata.namespace}.svc.cluster.local:10001"

    cluster = RayCluster(
        name=cluster_data["metadata"]["name"],
        namespace=cluster_data["metadata"]["namespace"],
        type=cluster_data["metadata"]["labels"]["evntl.io/ray-cluster-type"],
    )
    return RayClusterInfo(cluster=cluster, **info)


async def main():
    await config.load_kube_config()
    # await delete_ray_cluster(name="foobar", namespace="default")
    # print(await create_ray_cluster(name="foobar", namespace="default", cluster_type=ClusterType.SMALL))
    # print(await list_ray_clusters(namespace="default"))
    print(await describe_ray_cluster(name="foobar", namespace="default"))


if __name__ == "__main__":
    asyncio.run(main())
