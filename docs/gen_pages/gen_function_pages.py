import copy
import os
from dataclasses import dataclass

import griffe
import mkdocs_gen_files
import yaml
from jinja2 import Environment, FileSystemLoader

# Set up Jinja2 environment
template_dir = os.path.join(os.path.dirname(__file__), "templates")
env = Environment(loader=FileSystemLoader(template_dir))
module = griffe.load("daft.functions")


@dataclass
class FunctionCategory:
    title: str
    description: str
    functions: list[griffe.Function]


def build_function_category_map() -> list[FunctionCategory]:
    categories = {}

    not_functions = list()
    no_docstrings = set()
    for fn_name in module.exports:
        fn: griffe.Function = module[fn_name]
        if not fn.is_function:
            not_functions.append(fn_name)

        module_name = fn.module.name

        if module_name not in categories:
            if not fn.module.has_docstring:
                no_docstrings.add(fn.module.name)
                continue

            docstring = fn.module.docstring.value

            # get title and description from docstring
            docstring_split = docstring.split("\n", 1)
            if len(docstring_split) == 1:
                title = docstring_split[0]
                description = ""
            else:
                title = docstring_split[0]
                description = docstring_split[1]

            # strip period from title
            if title.endswith("."):
                title = title[:-1]

            cat = FunctionCategory(title, description, [])
            categories[module_name] = cat

        categories[module_name].functions.append(fn)

    if len(not_functions) > 0:
        raise ValueError(f"Expected all `daft.functions` exports to be functions, found: {not_functions}")

    if len(no_docstrings) > 0:
        raise ValueError(
            f'Missing docstrings on these modules in `daft.functions`: {no_docstrings}. Add docstrings like this to the top of the module file: \n"""<module_name> Functions.\n\nOptional description of module.\n"""'
        )

    categories_list = sorted(categories.values(), key=lambda cat: cat.title)
    for cat in categories_list:
        cat.functions = sorted(cat.functions, key=lambda fn: fn.name)

    return categories_list


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


def gen_index(categories: list[FunctionCategory]):
    categories_formatted = [
        {
            "title": cat.title,
            "description": cat.description,
            "functions": [
                {
                    "name": fn.name,
                    "signature": format_function_signature(fn),
                    "description": fn.docstring.value.split("\n")[0] if fn.docstring else "",
                }
                for fn in cat.functions
            ],
        }
        for cat in categories
    ]
    template = env.get_template("functions.md.j2")
    content = template.render(categories=categories_formatted)

    with mkdocs_gen_files.open("api/functions/index.md", "w") as f:
        f.write(content)


def build_function_toc(categories: list[FunctionCategory]) -> list:
    toc = [
        {
            "title": cat.title,
            "url": f"../#{cat.title.lower().replace(' ', '-')}",
            "children": [
                {
                    "title": fn.name,
                    "url": f"../{fn.name}/",
                }
                for fn in cat.functions
            ],
        }
        for cat in categories
    ]

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

    page_name = "index_" if fn.name == "index" else fn.name
    with mkdocs_gen_files.open(f"api/functions/{page_name}.md", "w") as f:
        f.write(content)


def gen_nav_summary():
    nav = mkdocs_gen_files.Nav()

    nav["Functions"] = "index.md"
    for fn_name in module.exports:
        page_name = "index_" if fn_name == "index" else fn_name
        nav[fn_name] = f"{page_name}.md"

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
