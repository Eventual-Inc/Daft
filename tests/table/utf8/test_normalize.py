from __future__ import annotations

import re
import string
import unicodedata

import pytest

from daft.expressions import col
from daft.table import MicroPartition


# source: RedPajama
def manual_normalize(text, remove_punct, lowercase, nfd_unicode, white_space):
    if text is None:
        return None

    if remove_punct:
        text = text.translate(str.maketrans("", "", string.punctuation))

    if lowercase:
        text = text.lower()

    if white_space:
        text = text.strip()
        text = re.sub(r"\s+", " ", text)

    if nfd_unicode:
        text = unicodedata.normalize("NFD", text)

    return text


NORMALIZE_TEST_DATA = [
    "regular text no changes",
    "Regular texT WITH uPpErCaSe",
    "ALL UPPERCASE TEXT",
    "text, with... punctuation!!!",
    "!&# #%*!()%*@# &*%#& @*( #*(@%()))",
    "!@#$%^&*()+_-=~`[]\\/.,';?><\":|}{",
    "UPPERCASE, AND, PUNCTUATION!?",
    "füñķÿ úňìčõðė",
    "füñķÿ, úňìčõðė!",
    "FüÑķŸ úňÌčõÐė",
    "FüÑķŸ,   úňÌčõÐė!",
    "way    too    much      space",
    "     space  all     over   the place    ",
    "other\ntypes\tof\r\nspace characters",
    "too\n\n\t\r\n  \n\t\tmuch\n\t\tspace\t  \n\n  \t\r\n \t",
    None,
    "TOO\n\n\t\r\n  \n\t\tMUCH\n\t\tsPACe\t  \n\n  \t\r\n \t",
    "too,\n\n?\t\r\n  \n\t\tmuc%h!!%\n\t\t\\SPACE??!\t  \n\n  \t\r\n \t",
    "FüÑķŸ,   úňÌčõÐė!   AND EVERYTHING else TOO    \t\na\t\t\nbCDe 😃😌😝",
    "",
    "specialcase",
    "SPECIALCASE",
    "😃",
    None,
]


@pytest.mark.parametrize("remove_punct", [False, True])
@pytest.mark.parametrize("lowercase", [False, True])
@pytest.mark.parametrize("nfd_unicode", [False, True])
@pytest.mark.parametrize("white_space", [False, True])
def test_utf8_normalize(remove_punct, lowercase, nfd_unicode, white_space):
    table = MicroPartition.from_pydict({"col": NORMALIZE_TEST_DATA})
    result = table.eval_expression_list(
        [
            col("col").str.normalize(
                remove_punct=remove_punct,
                lowercase=lowercase,
                nfd_unicode=nfd_unicode,
                white_space=white_space,
            )
        ]
    )
    expected = [manual_normalize(t, remove_punct, lowercase, nfd_unicode, white_space) for t in NORMALIZE_TEST_DATA]
    assert result.to_pydict() == {"col": expected}
