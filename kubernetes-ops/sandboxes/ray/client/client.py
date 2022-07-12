import copy
import functools
import json
from typing import Any, Callable, List, Optional, TypeVar, overload

import tenacity
import yaml
from kubernetes import client, config

T = TypeVar("T")
FuncT = Callable[..., T]
FuncOptT = Callable[..., Optional[T]]


def _should_retry(e: Exception) -> bool:
    # Retry on transient timeouts
    if isinstance(e, TimeoutError):
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
        def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
            try:
                # TODO: tune retry policy
                retry_decorator = tenacity.retry(
                    stop=tenacity.stop_after_attempt(10),
                    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10) + tenacity.wait_random(0, 2),
                    retry=tenacity.retry_if_exception(_should_retry),
                    reraise=True,
                )

                return retry_decorator(func)(*args, **kwargs)

            except client.rest.ApiException as e:
                if ignore is None or e.status not in ignore:
                    raise
                return None

        return wrapper

    return decorator


# TODO: hot reload from k8s configmap?
@functools.lru_cache(1)
def load_ray_cluster_template():
    with open("config.yaml") as f:
        return yaml.safe_load(f)


# TODO: what else do we want to parametrize?
def create_ray_cluster(
    *, name: str, namespace: str, head_cpu: int, head_memory: str, worker_cpu: int, worker_memory: str, max_workers: int
):
    template = copy.deepcopy(load_ray_cluster_template())

    # Set name
    template["metadata"]["name"] = name

    # Set head node resources
    head_group_container = next(
        filter(lambda d: d["name"] == "ray-head", template["spec"]["headGroupSpec"]["template"]["spec"]["containers"])
    )
    head_group_container["resources"] = {"limits": {"cpu": str(head_cpu), "memory": head_memory}}

    # Set worker max replicas
    # TODO: allow a single worker group for now?
    worker_group_spec = template["spec"]["workerGroupSpecs"][0]
    # TODO: make desired replicas and minReplicas configurable?
    # TODO: set limits based on total CPU/memory usage
    worker_group_spec["maxReplicas"] = max_workers
    # Set worker node resources
    worker_group_container = next(
        filter(
            lambda d: d["name"] == "ray-worker",
            worker_group_spec["template"]["spec"]["containers"],
        )
    )
    worker_group_container["resources"] = {"limits": {"cpu": str(worker_cpu), "memory": worker_memory}}

    api = client.CustomObjectsApi()
    retryable()(api.create_namespaced_custom_object)(
        group="ray.io",
        version="v1alpha1",
        plural="rayclusters",
        namespace=namespace,
        body=template,
    )


def delete_ray_cluster(*, name: str, namespace: str):
    api = client.CustomObjectsApi()
    retryable(ignore=[404])(api.delete_namespaced_custom_object)(
        group="ray.io", version="v1alpha1", plural="rayclusters", name=name, namespace=namespace
    )


def list_ray_clusters(*, namespace: str):
    api = client.CustomObjectsApi()
    items = retryable()(api.list_namespaced_custom_object)(
        group="ray.io", version="v1alpha1", plural="rayclusters", namespace=namespace
    )["items"]
    return [{"name": i["metadata"]["name"], "status": i.get("status", {})} for i in items]


def _get_events_for_object(*, uid: str, namespace: str):
    api = client.EventsV1beta1Api()

    class FakeEventTime:
        def __get__(self, obj, objtype=None):
            return obj._event_time

        def __set__(self, obj, value):
            obj._event_time = value

    # Monkey-patch the `event_time` attribute of ` the V1beta1Event class.
    # See: https://github.com/kubernetes-client/python/issues/1826
    # Fix from: https://stackoverflow.com/a/72591958
    client.V1beta1Event.event_time = FakeEventTime()
    items = retryable()(api.list_namespaced_event)(namespace="default", field_selector=f"regarding.uid={uid}").items
    return [
        {
            "timestamp": i.metadata.creation_timestamp.isoformat(),
            **{k: getattr(i, k) for k in ("note", "reason", "type")},
        }
        for i in items
    ]


def describe_ray_cluster(*, name: str, namespace: str):
    api = client.CustomObjectsApi()
    data = retryable()(api.get_namespaced_custom_object)(
        group="ray.io", version="v1alpha1", plural="rayclusters", name=name, namespace=namespace
    )
    data["status"] = status = data.get("status", {})
    status["events"] = _get_events_for_object(uid=data["metadata"]["uid"], namespace=namespace)
    return data


def main():
    config.load_kube_config()
    # delete_ray_cluster(name="foobar", namespace="default")
    # create_ray_cluster(
    #     name="foobar",
    #     namespace="default",
    #     head_cpu=1,
    #     head_memory="512M",
    #     worker_cpu=1,
    #     worker_memory="512M",
    #     max_workers=2,
    # )
    # print(list_ray_clusters(namespace="default"))
    print(json.dumps(describe_ray_cluster(name="foobar", namespace="default"), indent=4))


if __name__ == "__main__":
    main()
