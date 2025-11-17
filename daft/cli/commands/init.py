"""Init command for Daft CLI."""

from __future__ import annotations

import os
import pathlib
from typing import Any, Callable

import click
import questionary
from jinja2 import Environment, FileSystemLoader
from rich.console import Console
from rich.syntax import Syntax

import daft
from daft.functions import decode_image, file

console = Console()


def _prompt_with_cancellation(prompt_func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Helper to handle cancellation for prompts."""
    result = prompt_func(*args, **kwargs).ask()
    if result is None:
        console.print("[yellow]Initialization cancelled.[/yellow]")
    return result


def _load_data_source(data_source: str, project_type: str) -> tuple[daft.DataFrame | None, str | None]:
    """Load data from the specified source and return DataFrame and path/repo."""
    if data_source == "Other":
        # For "Other", we can't load sample data, so return None
        # This will trigger manual column entry in column selection
        return None, None
    elif data_source == "Huggingface":
        repo_name = _prompt_with_cancellation(questionary.text, "Enter Huggingface repo:")
        if repo_name is None:
            return None, None

        console.print()
        console.print("[bold]Loading sample data...[/bold]")
        try:
            df = daft.read_huggingface(repo_name)
            console.print()
            df.show()
            return df, repo_name
        except Exception:
            console.print(
                "[yellow]Could not load sample data. You can add credentials in the generated script if needed.[/yellow]"
            )
            return None, repo_name
    else:
        path = _prompt_with_cancellation(questionary.text, f"Enter path to {data_source} file or directory:")
        if path is None:
            return None, None

        console.print()
        console.print("[bold]Loading sample data...[/bold]")
        try:
            if data_source == "CSV":
                df = daft.read_csv(path)
            elif data_source == "JSONL":
                df = daft.read_json(path)
            elif data_source == "Parquet":
                df = daft.read_parquet(path)
            elif data_source == "Raw Files":
                df = _load_raw_files(path, project_type)
            else:
                return None, path

            console.print()
            df.show()
            return df, path
        except Exception:
            console.print(
                "[yellow]Could not load sample data. You can add credentials in the generated script if needed.[/yellow]"
            )
            return None, path


def _load_raw_files(path: str, project_type: str) -> daft.DataFrame:
    """Load raw files and create content column based on project type."""
    df = daft.from_glob_path(path)

    if project_type in ["Text Embedding", "Text Classification"]:
        df = df.with_column("content", df["path"].download().cast(str))
    elif project_type in ["Image Embedding", "Image Classification"]:
        df = df.with_column("content", decode_image(df["path"].download()))
    elif project_type == "Structured Output Generation":
        df = df.with_column("content", file(df["path"]))

    return df


def _select_text_column(schema: daft.Schema | None) -> list[str] | None:
    """Select a single string column for text tasks."""
    if schema is None:
        # Data loading failed, ask user to manually enter column name
        column_name = _prompt_with_cancellation(questionary.text, "Enter the name of the column to use for the task:")
        if column_name is None:
            return None
        return [column_name.strip()]

    all_columns = schema.column_names()
    available_columns = [col_name for col_name in all_columns if schema[col_name].dtype == daft.DataType.string()]

    if not available_columns:
        console.print("[yellow]No string columns found in the data.[/yellow]")
        return None

    selected_column = _prompt_with_cancellation(
        questionary.select,
        "Select a column to use for the task:",
        choices=available_columns,
    )

    if selected_column is None:
        return None

    return [selected_column]


def _select_image_column(schema: daft.Schema | None) -> list[str] | None:
    """Select a single string or binary column for image tasks."""
    if schema is None:
        # Data loading failed, ask user to manually enter column name
        column_name = _prompt_with_cancellation(questionary.text, "Enter the name of the column to use for the task:")
        if column_name is None:
            return None
        return [column_name.strip()]

    all_columns = schema.column_names()
    available_columns = []
    column_info = []

    for col_name in all_columns:
        dtype = schema[col_name].dtype
        if dtype == daft.DataType.string():
            available_columns.append(col_name)
            column_info.append(f"{col_name} (string - file paths)")
        elif dtype == daft.DataType.binary():
            available_columns.append(col_name)
            column_info.append(f"{col_name} (binary - image contents)")

    if not available_columns:
        console.print("[yellow]No string or binary columns found in the data.[/yellow]")
        return None

    selected_column = _prompt_with_cancellation(
        questionary.select,
        "Select a column to use for the task:",
        choices=column_info,
    )

    if selected_column is None:
        return None

    # Extract column name from the selected choice
    selected_column = selected_column.split(" (")[0]
    return [selected_column]


def _select_structured_columns(schema: daft.Schema | None) -> list[str] | None:
    """Select one or more columns for structured output tasks."""
    if schema is None:
        # Data loading failed, ask user to manually enter column names
        column_names_str = _prompt_with_cancellation(
            questionary.text, "Enter the names of the columns to use for the task (comma-separated):"
        )
        if column_names_str is None:
            return None
        # Split by comma and strip whitespace
        column_names = [name.strip() for name in column_names_str.split(",") if name.strip()]
        if not column_names:
            return None
        return column_names

    all_columns = schema.column_names()

    if not all_columns:
        console.print("[yellow]No columns found in the data.[/yellow]")
        return None

    selected_columns = _prompt_with_cancellation(
        questionary.checkbox,
        "Select one or more columns to use for the task:",
        choices=all_columns,
    )

    if selected_columns is None or len(selected_columns) == 0:
        return None

    return selected_columns


def _build_template_context(
    project_type: str,
    data_source: str,
    data_source_path: str | None,
    selected_columns: list[str],
    labels: list[str] | None,
    system_prompt: str | None,
    pydantic_model_code: str | None,
    output_column: str,
    output_data_source: str,
    python_file_name: str,
) -> dict[str, Any]:
    """Build context dictionary for template rendering."""
    return {
        "project_type": project_type,
        "data_source": data_source,
        "data_source_path": data_source_path,
        "selected_columns": selected_columns,
        "labels": labels,
        "system_prompt": system_prompt,
        "pydantic_model_code": pydantic_model_code,
        "output_column": output_column,
        "output_data_source": output_data_source,
        "python_file_name": python_file_name,
    }


def _render_and_write_template(context: dict[str, Any], python_file_name: str) -> None:
    """Render Jinja2 template and write to file."""
    # Map project type to template file name
    template_map = {
        "Text Embedding": "text_embedding.j2",
        "Image Embedding": "image_embedding.j2",
        "Text Classification": "text_classification.j2",
        "Image Classification": "image_classification.j2",
        "Structured Output Generation": "structured_output.j2",
    }

    template_name = template_map.get(context["project_type"])
    if not template_name:
        console.print(f"[red]Unknown project type: {context['project_type']}[/red]")
        return

    # Get template directory path
    template_dir = pathlib.Path(__file__).parent.parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)))

    # Add custom filter for Python list formatting
    def python_list(value: Any) -> str:
        """Format a Python list for use in Python code."""
        if value is None:
            return "None"
        if isinstance(value, list):
            return "[" + ", ".join(f'"{item}"' if isinstance(item, str) else str(item) for item in value) + "]"
        return str(value)

    env.filters["python_list"] = python_list

    try:
        template = env.get_template(template_name)
        rendered_code = template.render(**context)

        # Write to file
        with open(python_file_name, "w") as f:
            f.write(rendered_code)

        console.print(f"[green]Generated Python script: {python_file_name}[/green]")
    except Exception as e:
        console.print(f"[red]Error generating script: {e}[/red]")


def _get_output_data_sources(project_type: str) -> list[str]:
    """Get list of output data sources based on project type."""
    # Base output sources (all project types)
    base_sources = [
        "Parquet",
        "CSV",
        "JSON",
        "Iceberg",
        "Delta Lake",
        "Lance",
        "Clickhouse",
        "Huggingface",
        "Bigtable",
    ]

    # For embedding tasks, add Turbopuffer at the top
    if project_type in ["Text Embedding", "Image Embedding"]:
        return ["Turbopuffer"] + base_sources

    return base_sources


def _generate_pydantic_model() -> str | None:
    """Generate Pydantic model using OpenAI with iterative refinement.

    Returns:
        str | None: The final accepted Pydantic model code, or None if skipped/cancelled.
    """
    description = _prompt_with_cancellation(
        questionary.text, "Describe the structure of the output you want from the data (press Enter to skip):"
    )

    if description is None:
        return None

    description = description.strip() if description else None

    # If no description provided, return empty model
    if not description:
        return "class StructuredOutput(BaseModel):\n    pass"

    # Generate model with OpenAI and enter refinement loop
    try:
        from openai import OpenAI
    except ImportError:
        console.print("[yellow]OpenAI package not installed. Install it with: pip install openai[/yellow]")
        return None

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        console.print("[yellow]OPENAI_API_KEY environment variable not set. Skipping model generation.[/yellow]")
        return None

    client = OpenAI(api_key=api_key)
    messages = [
        {
            "role": "system",
            "content": "You are a helpful assistant that generates Pydantic models for use in the `response_format` field of OpenAI chat completions calls. The class must be named `StructuredOutput`. Return only the Python code for the Pydantic model class definition, without any imports or markdown formatting. Do not include import statements - just the class definition.",
        },
        {"role": "user", "content": f"Generate a Pydantic model for the following description: {description}"},
    ]

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.7,
        )
        model_code = response.choices[0].message.content.strip()

        # Remove markdown code blocks if present
        if model_code.startswith("```python"):
            model_code = model_code[9:]
        if model_code.startswith("```"):
            model_code = model_code[3:]
        if model_code.endswith("```"):
            model_code = model_code[:-3]
        model_code = model_code.strip()
    except Exception as e:
        console.print(f"[yellow]Error generating model: {e}[/yellow]")
        return None

    # Iterative refinement loop
    while True:
        console.print()
        console.print("[bold]Generated Pydantic model:[/bold]")
        console.print()
        syntax = Syntax(model_code, "python", theme="monokai", line_numbers=False)
        console.print(syntax)
        console.print()

        accept = _prompt_with_cancellation(
            questionary.select,
            "Do you want to accept this model?",
            choices=["Yes", "No, I want to make changes"],
        )

        if accept == "Yes":
            return model_code
        elif accept is None:
            return None
        else:
            changes = _prompt_with_cancellation(questionary.text, "What changes would you like to make?")

            if changes is None:
                return None

            # Add to conversation history
            messages.append({"role": "assistant", "content": model_code})
            messages.append({"role": "user", "content": f"Please modify the model with these changes: {changes}"})

            try:
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=messages,
                    temperature=0.7,
                )
                model_code = response.choices[0].message.content.strip()

                # Remove markdown code blocks if present
                if model_code.startswith("```python"):
                    model_code = model_code[9:]
                if model_code.startswith("```"):
                    model_code = model_code[3:]
                if model_code.endswith("```"):
                    model_code = model_code[:-3]
                model_code = model_code.strip()
            except Exception as e:
                console.print(f"[yellow]Error generating model: {e}[/yellow]")
                return None


@click.command()  # type: ignore[misc]
@click.help_option("-h", "--help")  # type: ignore[misc]
def init() -> None:
    """Initialize a new Daft project."""
    os.environ["DAFT_PROGRESS_BAR"] = "0"

    console.print("[bold green]Initializing Daft project...[/bold green]")
    console.print()

    # Step 1: Project type selection
    project_types = [
        "Text Embedding",
        "Image Embedding",
        "Text Classification",
        "Image Classification",
        "Structured Output Generation",
    ]
    project_type = _prompt_with_cancellation(
        questionary.select,
        "Select a project type:",
        choices=project_types,
    )

    if project_type is None:
        return

    console.print()

    # Step 2: Data source selection
    data_sources = ["CSV", "JSONL", "Parquet", "Huggingface", "Raw Files", "Other"]
    data_source = _prompt_with_cancellation(
        questionary.select,
        "Select a data source:",
        choices=data_sources,
    )

    if data_source is None:
        return

    console.print()

    # Step 3: Load data
    df, data_source_path = _load_data_source(data_source, project_type)

    # Step 4: Column selection
    console.print()
    schema = df.schema() if df is not None else None

    if project_type in ["Text Embedding", "Text Classification"]:
        selected_columns = _select_text_column(schema)
    elif project_type in ["Image Embedding", "Image Classification"]:
        selected_columns = _select_image_column(schema)
    elif project_type == "Structured Output Generation":
        selected_columns = _select_structured_columns(schema)
    else:
        selected_columns = None

    if selected_columns is None:
        return

    # Step 5: For classification tasks, ask for labels
    labels = None
    if project_type in ["Text Classification", "Image Classification"]:
        console.print()
        labels_str = _prompt_with_cancellation(questionary.text, "Enter the labels to classify into (comma-separated):")

        if labels_str is None:
            return

        # Split by comma and strip whitespace
        labels = [label.strip() for label in labels_str.split(",") if label.strip()]
        if not labels:
            console.print("[yellow]No labels provided. Initialization cancelled.[/yellow]")
            return

    # Step 6: For structured output, ask for system prompt and generate Pydantic model
    if project_type == "Structured Output Generation":
        console.print()
        system_prompt = _prompt_with_cancellation(
            questionary.text, "Enter an optional system prompt (press Enter to skip):"
        )

        if system_prompt is None:
            return

        # Use empty string if user just pressed Enter
        system_prompt = system_prompt.strip() if system_prompt else None

        pydantic_model_code = _generate_pydantic_model()
        if pydantic_model_code is None:
            return
    else:
        system_prompt = None
        pydantic_model_code = None

    # Step 7: Ask for output column name
    console.print()

    # Determine default based on project type
    if project_type in ["Text Embedding", "Image Embedding"]:
        default_output_column = "vector"
    elif project_type in ["Text Classification", "Image Classification"]:
        default_output_column = "class"
    elif project_type == "Structured Output Generation":
        default_output_column = "structured_output"
    else:
        default_output_column = "output"

    output_column = _prompt_with_cancellation(
        questionary.text,
        "Enter the name of the output column:",
        default=default_output_column,
    )

    if output_column is None:
        return

    output_column = output_column.strip()

    # Step 8: Ask for output data source
    console.print()
    output_data_sources = _get_output_data_sources(project_type)
    output_data_source = _prompt_with_cancellation(
        questionary.select,
        "Select an output data source:",
        choices=output_data_sources,
    )

    if output_data_source is None:
        return

    # Step 9: Ask for Python file name
    console.print()
    python_file_name = _prompt_with_cancellation(
        questionary.text,
        "Enter the Python file name:",
        default="main.py",
    )

    if python_file_name is None:
        return

    python_file_name = python_file_name.strip()

    # Step 10: Generate Python script from template
    context = _build_template_context(
        project_type=project_type,
        data_source=data_source,
        data_source_path=data_source_path,
        selected_columns=selected_columns,
        labels=labels,
        system_prompt=system_prompt,
        pydantic_model_code=pydantic_model_code,
        output_column=output_column,
        output_data_source=output_data_source,
        python_file_name=python_file_name,
    )

    _render_and_write_template(context, python_file_name)

    console.print()
    console.print("[bold green]✓ Project initialization complete![/bold green]")
    console.print()
    console.print("[bold]Next steps:[/bold]")
    console.print()
    console.print("1. Review the generated script and update any TODO items:")
    console.print(f"   [dim]   - Open {python_file_name} in your editor[/dim]")
    console.print("   [dim]   - Configure API keys, paths, and credentials as needed[/dim]")
    console.print()
    console.print("2. Run the script:")
    console.print(f"   [bold]   python {python_file_name}[/bold]")
