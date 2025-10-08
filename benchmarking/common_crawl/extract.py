from __future__ import annotations

import ftfy
import ray
from bs4 import BeautifulSoup

import daft

ray.init()
daft.context.set_runner_ray()


def process_html(html_bytes: bytes | None):
    """Process a single HTML content and extract text."""
    if html_bytes is None:
        return {"extracted_text": None, "text_length": 0}

    try:
        html_str = html_bytes.decode("utf-8")
        if not html_str:
            return {"extracted_text": None, "text_length": 0}

        # Use OWM to extract plain text
        response = BeautifulSoup(html_str, "html.parser").get_text()
        if response is None:
            return {"extracted_text": None, "text_length": 0}

        # Handle potential encoding issues
        try:
            if isinstance(response, str):
                fixed_response = ftfy.fix_text(response)
            else:
                return {"extracted_text": None, "text_length": 0}
        except Exception:
            return {"extracted_text": None, "text_length": 0}

        text_byte_len = len(fixed_response.encode("utf-8"))
        threshold = 100 * (2**20)  # 100MB in bytes

        if text_byte_len > threshold:
            return {"extracted_text": None, "text_length": text_byte_len}
        else:
            return {"extracted_text": fixed_response, "text_length": text_byte_len}

    except Exception:
        return {"extracted_text": None, "text_length": 0}


@daft.udf(
    return_dtype=daft.DataType.struct({"extracted_text": daft.DataType.string(), "text_length": daft.DataType.int32()}),
    batch_size=128,
)
def daft_extract_text(content: daft.Series):
    """Daft UDF to extract text from HTML content using OWM.

    Returns a struct with extracted_text and text_length.
    """
    # Get the config once for all rows
    return [process_html(html_bytes) for html_bytes in content]


# Read 640 CC warc.gz files
daft.read_warc(
    "s3://commoncrawl/crawl-data/CC-MAIN-2018-17/segments/1524125937193.1/warc/CC-MAIN-20180420081400-20180420101400-00*"
).with_column("extracted_text", daft_extract_text(daft.col("warc_content"))).exclude("warc_content").write_parquet(
    "extracted_text"
)
