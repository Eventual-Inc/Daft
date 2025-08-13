import copy
import os

import griffe
import mkdocs_gen_files
import yaml
from jinja2 import Environment, FileSystemLoader

# map of functions submodule to name displayed on docs
# also dictates category order
CATEGORY_TITLES = {"misc": "Miscellaneous", "columnar": "Columnar", "window": "Window", "llm": "LLM"}

# Set up Jinja2 environment
template_dir = os.path.join(os.path.dirname(__file__), "templates")
env = Environment(loader=FileSystemLoader(template_dir))
module = griffe.load("daft.functions")


def build_function_category_map() -> dict[str, list[griffe.Function]]:
    categories = {name: [] for name in CATEGORY_TITLES.keys()}

    for fn_name in module.exports:
        fn: griffe.Function = module[fn_name]
        if not fn.is_function:
            raise ValueError(f"Expected all `daft.functions` exports to be functions, found: {fn_name}")

        category = fn.module.name
        if category not in categories:
            raise ValueError(
                f"`daft.functions.{category}` not in category titles mapping. Add it in `docs/gen_pages/gen_function_pages.py`"
            )
        categories[category].append(fn)

    empty_categories = [k for k, v in categories.items() if len(v) == 0]
    if len(empty_categories) > 0:
        raise ValueError(
            f"These function submodules no longer exist: {empty_categories}. Remove them from the category titles mapping in `docs/gen_pages/gen_function_pages.py`"
        )

    categories = {
        CATEGORY_TITLES[name]: sorted(functions, key=lambda fn: fn.name) for name, functions in categories.items()
    }
    return categories


def format_function_signature(fn: griffe.Function) -> str:
    """Format function signature with optional parameters in brackets."""
    result_parts = []
    in_optional_group = False
    optional_group = []

    for param in fn.parameters:
        # Check if parameter has a default value (is optional)
        is_optional = param.default is not None and param.kind.name not in ["var_positional", "var_keyword"]
        is_var = param.kind.name in ["var_positional", "var_keyword"]

        if is_optional:
            if not in_optional_group:
                in_optional_group = True
                optional_group = [param.name]
            else:
                optional_group.append(param.name)
        else:
            # Close any open optional group
            if in_optional_group:
                result_parts.append("[, " + ", ".join(optional_group) + "]")
                in_optional_group = False
                optional_group = []

            # Add comma if needed
            if result_parts:
                result_parts.append(", ")

            # Add the parameter
            if is_var:
                if param.kind.name == "var_positional":
                    result_parts.append("*" + param.name)
                else:  # var_keyword
                    result_parts.append("**" + param.name)
            else:
                result_parts.append(param.name)

    # Close any remaining optional group
    if in_optional_group:
        result_parts.append("[, " + ", ".join(optional_group) + "]")

    return "".join(result_parts)


def gen_index(categories: dict[str, list[griffe.Function]]):
    categories_formatted = {
        name: [
            {
                "name": fn.name,
                "signature": format_function_signature(fn),
                "description": fn.docstring.value.split("\n")[0] if fn.docstring else "",
            }
            for fn in functions
        ]
        for name, functions in categories.items()
    }

    template = env.get_template("functions.md.j2")
    content = template.render(categories=categories_formatted)

    with mkdocs_gen_files.open("api/functions/index.md", "w") as f:
        f.write(content)


def build_function_toc(categories: dict[str, list[griffe.Function]]) -> list:
    toc = []
    for name, functions in categories.items():
        children = []
        for fn in functions:
            children.append(
                {
                    "title": fn.name,
                    "url": f"../{fn.name}/",
                }
            )

        toc.append({"title": name, "url": f"../#{name.lower()}", "children": children})

    return toc


def gen_function_page(fn: griffe.Function, toc: list):
    toc = copy.deepcopy(toc)
    for category in toc:
        for child in category["children"]:
            if child["title"] == fn.name:
                child["active"] = True

    meta = yaml.dump({"custom_toc": toc})

    template = env.get_template("function_page.md.j2")
    content = template.render(fn=fn, meta=meta)

    with mkdocs_gen_files.open(f"api/functions/{fn.name}.md", "w") as f:
        f.write(content)


def gen_nav_summary():
    nav = mkdocs_gen_files.Nav()

    nav["Functions"] = "index.md"
    for fn_name in module.exports:
        nav[fn_name] = f"{fn_name}.md"

    with mkdocs_gen_files.open("api/functions/SUMMARY.md", "w") as f:
        f.writelines(nav.build_literate_nav())


def main():
    categories = build_function_category_map()

    gen_nav_summary()
    gen_index(categories)

    toc = build_function_toc(categories)
    for fn_name in module.exports:
        gen_function_page(module[fn_name], toc)


main()
