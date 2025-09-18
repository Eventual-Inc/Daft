from __future__ import annotations

from typing import TYPE_CHECKING

from daft.context import get_context

if TYPE_CHECKING:
    from daft.logical.builder import LogicalPlanBuilder


class AsciiOptions:
    simple: bool

    def __init__(self, simple: bool = False):
        self.simple = simple


class SubgraphOptions:
    name: str
    subgraph_id: str
    metadata: str | None

    def __init__(self, name: str, subgraph_id: str, metadata: str | None = None):
        self.name = name
        self.subgraph_id = subgraph_id
        self.metadata = metadata


class MermaidOptions:
    simple: bool
    bottom_up: bool
    subgraph_options: SubgraphOptions | None

    def __init__(
        self, simple: bool = False, bottom_up: bool = False, subgraph_options: SubgraphOptions | None = None
    ) -> None:
        self.simple = simple
        self.bottom_up = bottom_up
        self.subgraph_options = subgraph_options

    def with_subgraph_options(self, name: str, subgraph_id: str, metadata: str | None = None) -> MermaidOptions:
        opts = MermaidOptions(self.simple, subgraph_options=SubgraphOptions(name, subgraph_id, metadata))

        return opts


class MermaidFormatter:
    def __init__(
        self, builder: LogicalPlanBuilder, show_all: bool = False, simple: bool = False, is_cached: bool = False
    ):
        self.builder = builder
        self.show_all = show_all
        self.simple = simple
        self.is_cached = is_cached

    def _repr_markdown_(self) -> str:
        builder = self.builder
        output = ""
        display_opts = MermaidOptions(simple=self.simple)
        # TODO handle cached plans
        if self.show_all:
            output = "```mermaid\n"
            output += "flowchart TD\n"
            output += builder._builder.repr_mermaid(
                display_opts.with_subgraph_options(name="Unoptimized LogicalPlan", subgraph_id="unoptimized")
            )
            output += "\n"

            builder = builder.optimize()
            output += builder._builder.repr_mermaid(
                display_opts.with_subgraph_options(name="Optimized LogicalPlan", subgraph_id="optimized")
            )
            output += "\n"
            if get_context().get_or_create_runner().name != "native":
                physical_plan_scheduler = builder.to_physical_plan_scheduler(get_context().daft_execution_config)
                output += physical_plan_scheduler._scheduler.repr_mermaid(
                    display_opts.with_subgraph_options(name="Physical Plan", subgraph_id="physical")
                )
            else:
                from daft.execution.native_executor import NativeExecutor

                native_executor = NativeExecutor()
                output += native_executor._executor.repr_mermaid(
                    builder._builder,
                    get_context().daft_execution_config,
                    display_opts.with_subgraph_options(name="Physical Plan", subgraph_id="physical"),
                )
            output += "\n"
            output += "unoptimized --> optimized\n"
            output += "optimized --> physical\n"
            output += "```\n"

        else:
            output = "```mermaid\n"
            output += builder._builder.repr_mermaid(display_opts)
            output += "\n"
            output += "```\n"
            output += (
                "Set `show_all=True` to also see the Optimized and Physical plans. This will run the query optimizer."
            )

        return output

    def __repr__(self) -> str:
        return self._repr_markdown_()
