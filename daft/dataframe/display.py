from daft.context import get_context


class MermaidFormatter:
    def __init__(self, builder, show_all: bool = False, simple: bool = False, is_cached: bool = False):
        self.builder = builder
        self.show_all = show_all
        self.simple = simple
        self.is_cached = is_cached

    def _repr_markdown_(self):
        builder = self.builder
        output = ""
        display_opts = {
            "simple": self.simple,
            "format": "mermaid",
        }

        # TODO handle cached plans

        if self.show_all:
            output = "```mermaid\n"
            output += "flowchart TD\n"
            output += builder._builder.display_as(
                {**display_opts, "subgraph_id": "unoptimized", "name": "Unoptimized Logical Plan"}
            )
            output += "\n"

            builder = builder.optimize()
            output += builder._builder.display_as(
                {**display_opts, "subgraph_id": "optimized", "name": "Optimized Logical Plan"}
            )
            output += "\n"
            physical_plan_scheduler = builder.to_physical_plan_scheduler(get_context().daft_execution_config)
            output += physical_plan_scheduler._scheduler.display_as(
                {**display_opts, "subgraph_id": "physical", "name": "Physical Plan"}
            )
            output += "\n"
            output += "unoptimized --> optimized\n"
            output += "optimized --> physical\n"
            output += "```\n"

        else:
            output = "```mermaid\n"
            output += builder._builder.display_as(display_opts)
            output += "\n"
            output += "```\n"
            output += (
                "Set `show_all=True` to also see the Optimized and Physical plans. This will run the query optimizer."
            )

        return output

    def __repr__(self):
        return self._repr_markdown_()
