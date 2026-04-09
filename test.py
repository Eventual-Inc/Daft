from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    Job,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import error_message_run, parent_run
from openlineage.client.transport import HttpConfig, HttpTransport, Transport
from openlineage.client.uuid import generate_new_uuid
from typing_extensions import override

import daft
from daft import __version__ as daft_version
from daft.daft import QueryEndState
from daft.subscribers import Subscriber
from daft.subscribers.events import QueryFinished, QueryStarted

PRODUCER = "https://github.com/Eventual-Inc/Daft"

logger = logging.getLogger("daft-openlineage")
logging.basicConfig(level=logging.INFO)


class OpenLineageSubscriber(Subscriber):
    def __init__(
        self,
        *,
        namespace: str = "local",
        name_prefix: str = "daft_query",
        parent_job_namespace: str | None = None,
        parent_job_name: str | None = None,
        parent_run_id: str | None = None,
        root_job_namespace: str | None = None,
        root_job_name: str | None = None,
        root_run_id: str | None = None,
        # Override OpenLineageClient config
        config: dict[str, Any] | None = None,
        transport: Transport | None = None,
    ) -> None:
        """Construct a new OpenLineageSubscriber.

        Note that the subscriber uses the OpenLineageClient API to send events.
        The OpenLineageClient API automatically detects environment variables and
        YAML configuration files to configure the client. They can be overridden by
        passing the `config` parameter.

        See the OpenLineageClient Python API documentation for more details.
        https://openlineage.io/docs/client/python/

        Args:
            namespace: The namespace of the job.
            name_prefix: Prefix to apply to each job. Job names will be formatted as "{name_prefix}_{counter}"
                where counter is a monotonically increasing integer.
            parent_job_namespace: The namespace of the parent job.
            parent_job_name: The name of the parent job.
            parent_run_id: The run ID of the parent job.
            root_job_namespace: The namespace of the root job.
            root_job_name: The name of the root job.
            root_run_id: The run ID of the root job.
            config: Overridden OpenLineageClient config.
            transport: Overridden OpenLineageClient transport configuration. Recommended to configure
                this when connecting to your OpenLineage server.
        """
        self._client = OpenLineageClient(config=config, transport=transport)

        self._namespace = namespace
        self._name_prefix = name_prefix
        self._counter = 0
        self._parent = self._build_parent_facet(
            parent_job_namespace,
            parent_job_name,
            parent_run_id,
            root_job_namespace,
            root_job_name,
            root_run_id,
        )

        self._query_to_run_info: dict[str, tuple[str, str]] = {}

    @classmethod
    def from_env(cls) -> OpenLineageSubscriber:
        return cls()

    @staticmethod
    def _build_parent_facet(
        namespace: str | None,
        name: str | None,
        run_id: str | None,
        root_namespace: str | None,
        root_name: str | None,
        root_run_id: str | None,
    ) -> parent_run.ParentRunFacet | None:
        """Build the JSON for the parent facet in OpenLineage events."""
        if not any((namespace, name, run_id, root_namespace, root_name, root_run_id)):
            return None
        return parent_run.ParentRunFacet(
            job=parent_run.Job(
                name=name,
                namespace=namespace,
            ),
            run=parent_run.Run(
                runId=run_id,
            ),
            root=parent_run.Root(
                job=parent_run.RootJob(
                    name=root_name,
                    namespace=root_namespace,
                ),
                run=parent_run.RootRun(
                    runId=root_run_id,
                ),
            ),
        )

    @override
    def on_query_started(self, start_event: QueryStarted) -> None:
        run_id = str(generate_new_uuid())
        job_name = f"{self._name_prefix}_{self._counter}"
        self._query_to_run_info[start_event.query_id] = (run_id, job_name)
        self._counter += 1

        self._client.emit(
            RunEvent(
                eventType=RunState.START,
                eventTime=datetime.now(timezone.utc).isoformat(),
                producer=PRODUCER,
                run=Run(
                    runId=run_id,
                    facets={
                        "parent": self._parent,
                        "daft": {
                            "query_id": start_event.query_id,
                            "daft_version": daft_version,
                        },
                    },
                ),
                job=Job(
                    namespace=self._namespace,
                    name=job_name,
                    facets={
                        "daft": {
                            "entrypoint": start_event.metadata.entrypoint,
                            "unoptimized_plan": json.loads(start_event.metadata.unoptimized_plan),
                            "runner": start_event.metadata.runner,
                        }
                    },
                ),
            )
        )

    @override
    def on_query_finished(self, event: QueryFinished) -> None:
        run_id, job_name = self._query_to_run_info.get(event.query_id)
        if run_id is None:
            logger.warning("No OpenLineage run_id found for query_id=%s", event.query_id)
            return

        facets = {}

        match event.result.end_state:
            case QueryEndState.Finished:
                event_type = RunState.COMPLETE
            case QueryEndState.Canceled:
                event_type = RunState.ABORT
                facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                    message=event.result.error_message,
                    programmingLanguage="python",
                )
            case QueryEndState.Failed | QueryEndState.Dead:
                event_type = RunState.FAIL
                facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                    message=event.result.error_message,
                    programmingLanguage="rust",
                )

        self._client.emit(
            RunEvent(
                eventType=event_type,
                eventTime=datetime.now(timezone.utc).isoformat(),
                producer=PRODUCER,
                run=Run(runId=run_id, facets=facets),
                job=Job(
                    namespace=self._namespace,
                    name=job_name,
                ),
            )
        )

        del self._query_to_run_info[event.query_id]

    @override
    def close(self) -> None:
        self._query_to_run_info.close()
        self._client.close()


if __name__ == "__main__":
    # Set DAFT_RUNNER=native to ensure query lifecycle subscriber events are emitted.
    subscriber = OpenLineageSubscriber(
        namespace="local",
        name_prefix="daft_query",
        transport=HttpTransport(config=HttpConfig(url="http://localhost:9000")),
    )
    daft.attach_subscriber("openlineage", subscriber)
    daft.from_pydict({"x": [1, 2, 3], "y": [4, 5, 6]}).with_column("z", daft.col("x") + daft.col("y")).collect()
