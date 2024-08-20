from typing import Optional

from daft.context import get_context


class AsciiOptions:
    simple: bool

    def __init__(self, simple: bool = False):
        self.simple = simple


class SubgraphOptions:
    name: str
    subgraph_id: str

    def __init__(self, name: str, subgraph_id: str):
        self.name = name
        self.subgraph_id = subgraph_id


class MermaidOptions:
    simple: bool
    bottom_up: bool
    subgraph_options: Optional[SubgraphOptions]

    def __init__(self, simple: bool = False, bottom_up=False, subgraph_options: Optional[SubgraphOptions] = None):
        self.simple = simple
        self.bottom_up = bottom_up
        self.subgraph_options = subgraph_options

    def with_subgraph_options(self, name: str, subgraph_id: str):
        opts = MermaidOptions(self.simple, subgraph_options=SubgraphOptions(name, subgraph_id))

        return opts


class MermaidFormatter:
    def __init__(self, builder, show_all: bool = False, simple: bool = False, is_cached: bool = False):
        self.builder = builder
        self.show_all = show_all
        self.simple = simple
        self.is_cached = is_cached

    def _repr_markdown_(self):
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
            physical_plan_scheduler = builder.to_physical_plan_scheduler(get_context().daft_execution_config)
            output += physical_plan_scheduler._scheduler.repr_mermaid(
                display_opts.with_subgraph_options(name="Physical Plan", subgraph_id="physical")
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

    def __repr__(self):
        return self._repr_markdown_()
