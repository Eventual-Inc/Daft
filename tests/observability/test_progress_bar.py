from __future__ import annotations

import threading

import pytest

import daft
from daft.runners.progress_bar import SwordfishProgressBar


# Non-regression test for progress bar truncating UTF-8 pipeline names correctly.
# See: https://github.com/Eventual-Inc/Daft/actions/runs/21921434809
@pytest.mark.parametrize(
    "col_name",
    [
        "ñ" * 10,  # 20 bytes UTF-8 (each ñ = 2 bytes), truncation at byte 15 splits a char
        "日本語カラム名テスト",  # 30 bytes UTF-8 (each CJK char = 3 bytes)
        "🎉🎊🎈🎁🎂🎃",  # 24 bytes UTF-8 (each emoji = 4 bytes)
        "café_résumé_naïve",  # mixed ASCII and 2-byte chars
    ],
    ids=["two_byte_chars", "three_byte_cjk", "four_byte_emoji", "mixed_ascii_multibyte"],
)
def test_progress_bar_truncates_multibyte_utf8_pipeline_names(col_name):
    """Progress bar should not panic when truncating pipeline names with multi-byte UTF-8."""
    df = daft.from_pydict({col_name: [1.0, 2.0, 3.0]})
    # col + col is an "interesting" expression that survives optimizer constant-folding,
    # causing ProjectOperator to use the expression display name as the pipeline name.
    df = df.with_column(col_name, daft.col(col_name) + daft.col(col_name))
    result = df.collect()
    assert result.to_pydict()[col_name] == [2.0, 4.0, 6.0]


# Non-regression test for fd leaks in Jupyter: writing to stdout from native threads
# leaks an ipykernel zmq pipe (~2 fds) per write on Python 3.13+, because each call
# into Python from a native thread can get a fresh thread identity. All tqdm writes
# must therefore happen on a single long-lived Python thread, never the caller's.
# See: https://github.com/Eventual-Inc/Daft/issues/7253
def test_swordfish_progress_bar_writes_from_single_stable_thread():
    write_threads = set()

    class RecordingTqdm:
        def __init__(self, *args, **kwargs):
            write_threads.add(threading.get_ident())

        def set_description_str(self, desc):
            write_threads.add(threading.get_ident())

        def close(self):
            write_threads.add(threading.get_ident())

    bar = SwordfishProgressBar()
    bar.tqdm_mod = RecordingTqdm
    pb_id = bar.make_new_bar("🗡️ 🐟 test: {elapsed} {desc}")

    # Simulate updates arriving from short-lived foreign threads, each with a
    # distinct Python thread identity, as happens with native threads on 3.13+.
    caller_threads = []
    for i in range(5):
        t = threading.Thread(target=bar.update_bar, args=(pb_id, f"message {i}"))
        t.start()
        t.join()
        caller_threads.append(t.ident)
    bar.close()

    assert len(write_threads) == 1
    assert write_threads.isdisjoint(caller_threads)
