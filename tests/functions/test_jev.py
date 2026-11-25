from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import daft
from daft import DataType, col
from daft.functions import jev, jev_choice, jev_prob, jev_score, to_struct

pytest.importorskip("typesafe_sdk")


class _FakeSystemOne(BaseHTTPRequestHandler):
    """Deterministic stand-in for POST /v1/systemone."""

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append({"authorization": self.headers["Authorization"], **body})
        rows = body["state"]["rows"]
        answers = {name: _answer(q, json.dumps(rows[int(name[1:])])) for name, q in body["questions"].items()}
        answers = {name: a for name, a in answers.items() if a is not None}
        payload = json.dumps({"model": body["model"], "usage": {}, "answers": answers}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        pass


def _answer(question: dict, row: str) -> dict | None:
    if "omit" in row:
        return None
    if question["type"] == "noul":
        return {"type": "noul", "noul": 0.9 if "angry" in row else 0.1}
    if question["type"] == "choice":
        options = list(question["criteria"])
        choice = next((o for o in options if o in row), options[0])
        return {
            "type": "choice",
            "choice": choice,
            "confidence": 1.0,
            "probabilities": {o: float(o == choice) for o in options},
        }
    levels = question["criteria"]
    return {
        "type": "score",
        "score": 1.5,
        "confidence": 0.5,
        "legend": {str(i): level for i, level in enumerate(levels)},
        "probabilities": {str(i): 1.0 / len(levels) for i in range(len(levels))},
    }


@pytest.fixture(scope="module")
def server():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _FakeSystemOne)
    httpd.requests = []
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()


@pytest.fixture
def fake(server):
    server.requests.clear()
    server.kwargs = {"api_key": "test-key", "base_url": f"http://127.0.0.1:{server.server_port}"}
    return server


def test_jev_prob_and_jev(fake):
    df = daft.from_pydict({"ticket": ["I am angry", None, "all good"]})
    out = df.select(
        jev_prob(df["ticket"], "the customer is angry", **fake.kwargs).alias("p"),
        jev(df["ticket"], "the customer is angry", **fake.kwargs).alias("default"),
        jev(df["ticket"], "the customer is angry", threshold=0.05, **fake.kwargs).alias("low"),
    ).to_pydict()

    assert out == {
        "p": [0.9, None, 0.1],
        "default": [True, None, False],
        "low": [True, None, True],
    }
    # Null rows are never sent.
    request = fake.requests[0]
    assert request["authorization"] == "Bearer test-key"
    assert request["state"] == {"condition": "the customer is angry", "rows": ["I am angry", "all good"]}
    assert request["questions"]["r1"] == {
        "type": "noul",
        "instructions": "Does the record rows[1] satisfy the condition stated in condition?",
    }


def test_jev_nan_row_is_null(fake):
    df = daft.from_pydict({"x": [float("nan"), 1.0, None]})
    out = df.select(jev_prob(df["x"], "is positive", **fake.kwargs).alias("p")).to_pydict()

    assert out == {"p": [None, 0.1, None]}
    assert fake.requests[0]["state"]["rows"] == [1.0]


def test_jev_missing_answer_raises(fake):
    df = daft.from_pydict({"ticket": ["fine", "omit me"]})
    with pytest.raises(RuntimeError, match=r"no answers for \['r1'\]"):
        df.select(jev_prob(df["ticket"], "is fine", **fake.kwargs)).collect()


def test_jev_struct_row_sends_field_names(fake):
    df = daft.from_pydict({"name": ["Hans"], "age": [40]})
    df.select(jev_prob(to_struct(df["name"], df["age"]), "is an adult", **fake.kwargs)).collect()

    assert fake.requests[0]["state"]["rows"] == [{"name": "Hans", "age": 40}]


def test_jev_choice(fake):
    df = daft.from_pydict({"ticket": ["double billing charge", "crash on login", None]})
    df = df.select(jev_choice(df["ticket"], "which team?", ["billing", "technical"], **fake.kwargs).alias("team"))

    assert df.schema()["team"].dtype == DataType.string()
    assert df.to_pydict() == {"team": ["billing", "billing", None]}
    assert fake.requests[0]["questions"]["r0"] == {
        "type": "choice",
        "instructions": "For the record rows[0]: which team?",
        "criteria": {"billing": None, "technical": None},
    }


def test_jev_score(fake):
    df = daft.from_pydict({"product": ["stool", None]})
    levels = ["budget", "mid-range", "premium"]
    df = df.select(jev_score(df["product"], "how luxurious?", levels, **fake.kwargs).alias("tier"))

    assert df.schema()["tier"].dtype == DataType.float64()
    assert df.to_pydict() == {"tier": [1.5, None]}
    assert fake.requests[0]["questions"]["r0"]["criteria"] == levels


def test_jev_packs_rows_per_request(fake):
    df = daft.from_pydict({"ticket": [f"ticket {i}" for i in range(60)]})
    df.select(jev_prob(df["ticket"], "is urgent", **fake.kwargs)).collect()

    assert sorted(len(r["questions"]) for r in fake.requests) == [10, 25, 25]


def test_jev_settings_from_driver_env(fake, monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "env-key")
    monkeypatch.setenv("TYPESAFE_DEFAULT_MODEL", "env-model")
    monkeypatch.setenv("TYPESAFE_BASE_URL", fake.kwargs["base_url"])
    df = daft.from_pydict({"ticket": ["hi"]})
    df = df.select(jev_prob(df["ticket"], "is a greeting"))
    # Settings are captured when the expression is built, not read on workers.
    for name in ["TYPESAFE_API_KEY", "TYPESAFE_DEFAULT_MODEL", "TYPESAFE_BASE_URL"]:
        monkeypatch.delenv(name)
    df.collect()

    assert fake.requests[0]["authorization"] == "Bearer env-key"
    assert fake.requests[0]["model"] == "env-model"


def test_jev_missing_api_key(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(ValueError, match="TYPESAFE_API_KEY"):
        jev_prob(col("x"), "anything")


@pytest.mark.parametrize("n", [0, 256])
def test_jev_choice_option_count(n):
    with pytest.raises(ValueError, match="between 1 and 255 options"):
        jev_choice(col("x"), "q", [str(i) for i in range(n)], api_key="k")


def test_jev_choice_dedupes_options(fake):
    jev_choice(col("x"), "q", [str(i) for i in range(255)] + ["0"], api_key="k")

    df = daft.from_pydict({"ticket": ["hi"]})
    df.select(jev_choice(df["ticket"], "q", ["a", "b", "a"], **fake.kwargs)).collect()
    assert fake.requests[0]["questions"]["r0"]["criteria"] == {"a": None, "b": None}


@pytest.mark.parametrize("n", [1, 11])
def test_jev_score_level_count(n):
    with pytest.raises(ValueError, match="between 2 and 10 levels"):
        jev_score(col("x"), "q", [str(i) for i in range(n)], api_key="k")
