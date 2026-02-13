# Working with Documents

Documents are a common type of data that can be found in many different formats. The `daft.File` type is particularly useful for working with documents in a distributed manner.


## Prompting LLMs with Text Documents as Context

The `prompt` function supports multiple file input methods depending on the provider and file type:

- **PDF files**: Passed directly as file inputs (native OpenAI support)
- **Text files** (Markdown, HTML, CSV, etc.): Content is automatically extracted and injected into prompts
- **Images**: Supported via `daft.DataType.Image()` or file paths


For example, the following code demonstrates how to use `prompt` with `daft.File` to read a PDF file to search the web for closely related papers.

```python
import daft
from daft.functions import embed_text, prompt, file, regexp_split, unnest
from pydantic import BaseModel, Field


class Citation(BaseModel):
    url: str = Field(description="The URL of the source")
    title: str = Field(description="The title of the source")
    snippet: str = Field(description="A snippet of the source text")


class SearchResults(BaseModel):
    summary: str = Field(description="A summary of the search results")
    citations: list[Citation] = Field(description="A list of citations")


df = (
    daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/papers/*.pdf").limit(1)
    .with_column(
        "results",
        prompt(
            messages=[
                daft.lit("Find 5 closely related papers to the one attached"),
                file(daft.col("path")),
            ],
            model="gpt-4-turbo",
            tools=[{"type": "web_search"}],
            return_format=SearchResults,
            provider="openai",
            unnest=True,
        ),
    )
)
results = df.to_pydict()
print(results)
```

``` {title="Output"}
{
    'path': ['hf://datasets/Eventual-Inc/sample-files/papers/2102.04074v1.pdf'],
    'summary': [
        'Here are 5 closely related papers on scaling laws and learning-curve theory that complement Hutter (2021):

        - Deep Learning Scaling is Predictable, Empirically (Hestness et al., 2017). Early large-scale empirical study showing power-law error decreases with data/model/compute across multiple domains—motivating theory like Hutter’s. ([arxiv.org](https://arxiv.org/abs/1712.00409?utm_source=openai))
        - Scaling Laws for Neural Language Models (Kaplan et al., 2020). Establishes power-law scaling of loss with parameters, dataset size, and compute; provides simple formulas for compute-optimal tradeoffs. ([arxiv.org](https://arxiv.org/abs/2001.08361?utm_source=openai))
        - Scaling Laws for Autoregressive Generative Modeling (Henighan et al., 2020). Extends empirical scaling laws beyond text to images, video, and multimodal settings, reinforcing near-universality of power-law behavior. ([arxiv.org](https://arxiv.org/abs/2010.14701?utm_source=openai))
        - Explaining Neural Scaling Laws (Bahri, Dyer, Kaplan, Lee, Sharma, 2021). Provides a theoretical framework (variance‑limited vs. resolution‑limited regimes) that explains when and why power-law scaling with data/model size emerges—conceptually close to Hutter’s theory focus. ([arxiv.org](https://arxiv.org/abs/2102.06701?utm_source=openai))
        - Scaling Laws from the Data Manifold Dimension (Sharma & Kaplan, JMLR 2022). Theoretical account linking scaling exponents to intrinsic data‑manifold dimension; offers explicit predictions for exponents observed empirically. ([jmlr.org](https://jmlr.org/papers/v23/20-1111.html?utm_source=openai))'
    ],
    'citations': [
        [
            {
                'url': 'https://arxiv.org/abs/1712.00409',
                'title': 'Deep Learning Scaling is Predictable, Empirically',
                'snippet': 'Empirical study showing power-law generalization error scaling across data, model, and compute.'
            }, {
                'url': 'https://arxiv.org/abs/2001.08361',
                'title': 'Scaling Laws for Neural Language Models',
                'snippet': 'Power-law scaling of cross-entropy loss with parameters, data, and compute; compute-optimal tradeoffs.'
            }, {
                'url': 'https://openai.com/research/scaling-laws-for-neural-language-models',
                'title': 'Scaling laws for neural language models | OpenAI',
                'snippet': 'Project page summarizing results and implications.'
            }, {
                'url': 'https://arxiv.org/abs/2010.14701',
                'title': 'Scaling Laws for Autoregressive Generative Modeling',
                'snippet': 'Empirical scaling across images, video, multimodal, and math domains.'
            }
        ]
    ]
}
```

## Reading a PDF file to extract text and image content from each page

For example, you can use `daft.File` to read a PDF file and extract the text and images from it.

```python
import daft
import pymupdf

@daft.func(
    return_dtype=daft.DataType.list(
        daft.DataType.struct(
            {
                "page_number": daft.DataType.uint8(),
                "page_text": daft.DataType.string(),
                "page_image_bytes": daft.DataType.binary(),
            }
        )
    )
)
def extract_pdf(file: daft.File):
    """Extracts the content of a PDF file."""
    pymupdf.TOOLS.mupdf_display_errors(False)  # Suppress non-fatal MuPDF warnings
    content = []
    with file.to_tempfile() as tmp:
        doc = pymupdf.Document(filename=str(tmp.name), filetype="pdf")
        for pno, page in enumerate(doc):
            row = {
                "page_number": pno,
                "page_text": page.get_text("text"),
                "page_image_bytes": page.get_pixmap().tobytes(),
            }
            content.append(row)
        return content

if __name__ == "__main__":
    # Discover and download pdfs
    df = (
        daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/papers/*.pdf")
        .with_column("pdf_file", daft.functions.file(daft.col("path")))
        .with_column("pages", extract_pdf(daft.col("pdf_file")))
        .explode("pages")
        .select(daft.col("path"), daft.functions.unnest(daft.col("pages")))
    )
    df.show(3)
```

``` {title="Output"}
╭────────────────────────────────┬─────────────┬────────────────────────────────┬────────────────────────────────╮
│ path                           ┆ page_number ┆ page_text                      ┆ page_image_bytes               │
│ ---                            ┆ ---         ┆ ---                            ┆ ---                            │
│ String                         ┆ UInt8       ┆ String                         ┆ Binary                         │
╞════════════════════════════════╪═════════════╪════════════════════════════════╪════════════════════════════════╡
│ hf://datasets/Eventual-Inc/sa… ┆ 0           ┆ Learning Curve Theory          ┆ b"\x89PNG\r\n\x1a\n\x00\x00\x… │
│                                ┆             ┆ Marcus …                       ┆                                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sa… ┆ 1           ┆ 1                              ┆ b"\x89PNG\r\n\x1a\n\x00\x00\x… │
│                                ┆             ┆ Introduction                   ┆                                │
│                                ┆             ┆ Power laws in …                ┆                                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sa… ┆ 2           ┆ Theory: Scaling with data si…  ┆ b"\x89PNG\r\n\x1a\n\x00\x00\x… │
╰────────────────────────────────┴─────────────┴────────────────────────────────┴────────────────────────────────╯

(Showing first 3 rows)
```

## Extracting Structure and Content from Markdown

The following example demonstrates how to use `daft.File` to extract the structure and content from a Markdown file.

```python
import daft
from daft import DataType
import re


@daft.func(
    return_dtype=DataType.struct(
        {
            "title": DataType.string(),
            "content": DataType.string(),
            "headings": DataType.list(
                DataType.struct(
                    {
                        "level": DataType.int64(),
                        "text": DataType.string(),
                        "line": DataType.int64(),
                    }
                )
            ),
            "code_blocks": DataType.list(
                DataType.struct(
                    {
                        "language": DataType.string(),
                        "code": DataType.string(),
                    }
                )
            ),
            "links": DataType.list(
                DataType.struct(
                    {
                        "text": DataType.string(),
                        "url": DataType.string(),
                    }
                )
            ),
        }
    )
)
def extract_markdown(file: daft.File):
    """Extract structure and content from a Markdown file."""
    with file.open() as f:
        content = f.read().decode("utf-8")

    # Extract title (first h1 heading)
    title_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
    title = title_match.group(1).strip() if title_match else None

    # Extract all headings with their levels and line numbers
    headings = []
    for i, line in enumerate(content.split("\n"), start=1):
        heading_match = re.match(r"^(#{1,6})\s+(.+)$", line)
        if heading_match:
            headings.append({
                "level": len(heading_match.group(1)),
                "text": heading_match.group(2).strip(),
                "line": i,
            })

    # Extract code blocks with language
    code_blocks = []
    code_pattern = re.compile(r"```(\w*)\n(.*?)```", re.DOTALL)
    for match in code_pattern.finditer(content):
        code_blocks.append({
            "language": match.group(1) or "text",
            "code": match.group(2).strip(),
        })

    # Extract links [text](url)
    links = []
    link_pattern = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
    for match in link_pattern.finditer(content):
        links.append({
            "text": match.group(1),
            "url": match.group(2),
        })

    return {
        "title": title,
        "content": content,
        "headings": headings,
        "code_blocks": code_blocks,
        "links": links,
    }


if __name__ == "__main__":
    from daft import col
    from daft.functions import file, unnest

    # Discover Markdown files
    df = (
        daft.from_glob_path("~/git/Daft/**/*.md")
        .with_column("file", file(col("path")))
        .with_column("markdown", extract_markdown(col("file")))
        .select(col("path"), unnest(col("markdown")))
    )

    df.show(3)
```

``` {title="Output"}
╭─────────────────────────┬─────────────────────────┬────────────────────────┬────────────────────────┬────────────────────────┬────────────────────────╮
│ path                    ┆ title                   ┆ content                ┆ headings               ┆ code_blocks            ┆ links                  │
│ ---                     ┆ ---                     ┆ ---                    ┆ ---                    ┆ ---                    ┆ ---                    │
│ String                  ┆ String                  ┆ String                 ┆ List[Struct[level:     ┆ List[Struct[language:  ┆ List[Struct[text:      │
│                         ┆                         ┆                        ┆ Int64, text: String,   ┆ String, code: String]] ┆ String, url: String]]  │
│                         ┆                         ┆                        ┆ line: Int64]]          ┆                        ┆                        │
╞═════════════════════════╪═════════════════════════╪════════════════════════╪════════════════════════╪════════════════════════╪════════════════════════╡
│ file:///Users/everettkl ┆ Contributor Covenant    ┆ # Contributor Covenant ┆ [{level: 1,            ┆ []                     ┆ [{text: Mozilla's code │
│ even/g…                 ┆ Code of …               ┆ Code o…                ┆ text: Contributor…     ┆                        ┆ of con…                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ file:///Users/everettkl ┆ Contributing to Daft    ┆ # Contributing to Daft ┆ [{level: 1,            ┆ []                     ┆ [{text: Report it      │
│ even/g…                 ┆                         ┆                        ┆ text: Contrib…         ┆                        ┆ here,                  │
│                         ┆                         ┆ Daft …                 ┆                        ┆                        ┆ url: …                 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ file:///Users/everettkl ┆ Resources               ┆ # Resources            ┆ [{level: 1,            ┆ []                     ┆ [{text: Testing        │
│ even/g…                 ┆                         ┆                        ┆ text: Resources,       ┆                        ┆ Details,               │
│                         ┆                         ┆ - https://docs.d…      ┆ …                      ┆                        ┆ url:…                  │
╰─────────────────────────┴─────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────╯

(Showing first 3 rows)
```
