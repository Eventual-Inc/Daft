"""Tests for the Celeborn shuffle backend.

These cover the Python config surface (`daft.execution_config_ctx`) for the
Celeborn options: that ``shuffle_algorithm="celeborn"`` is accepted, that each
option is validated, that they round-trip onto ``DaftExecutionConfig``, and that
a Celeborn run this build cannot honour fails instead of quietly falling back to
another shuffle. They run against any runner.

The map/push/fetch/reduce path itself is covered on the Rust side, which can
exercise it without a Celeborn cluster; see the ``celeborn`` tests in
``daft-shuffles`` and ``daft-local-execution``.
"""

from __future__ import annotations

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

###
# Configuration-layer tests.
#
# These run on every runner. They pin the Python config surface so that any
# accidental rename or removal of a Celeborn option is caught by a fast,
# environment-free unit test.
###


def test_celeborn_shuffle_algorithm_is_accepted():
    """`shuffle_algorithm="celeborn"` must be a valid value.

    It was added to the validation whitelist alongside `flight_shuffle`.
    """
    with daft.execution_config_ctx(
        shuffle_algorithm="celeborn",
        celeborn_lm_host="host",
        celeborn_lm_port=9097,
    ):
        # Reaching here without ValueError is the assertion.
        pass


def test_celeborn_lm_host_must_not_be_empty():
    """An empty celeborn_lm_host is a configuration mistake.

    The validator in daft-config/src/python.rs must surface it as ValueError so
    users get a clear failure at config time instead of a runtime crash.
    """
    with (
        pytest.raises(ValueError, match="celeborn_lm_host"),
        daft.execution_config_ctx(
            shuffle_algorithm="celeborn",
            celeborn_lm_host="   ",
        ),
    ):
        pass


def test_celeborn_connection_params_round_trip():
    """The LifecycleManager coordinates must survive the round-trip to Rust."""
    with daft.execution_config_ctx(
        shuffle_algorithm="celeborn",
        celeborn_lm_host="host",
        celeborn_lm_port=9097,
        celeborn_app_id="my-app",
    ):
        cfg = daft.context.get_context().daft_execution_config
        assert cfg.celeborn_lm_host == "host"
        assert cfg.celeborn_lm_port == 9097
        assert cfg.celeborn_app_id == "my-app"


def test_celeborn_lm_port_must_be_a_valid_port():
    """An out-of-range celeborn_lm_port must be rejected at config time.

    The field is an i32 all the way down to the FFI, so nothing below would
    reject `0` or a negative value on its own; a bad port would instead surface
    as an opaque connection failure at shuffle time.
    """
    for bad_port in (0, -1, 70000):
        with (
            pytest.raises(ValueError, match="celeborn_lm_port"),
            daft.execution_config_ctx(
                shuffle_algorithm="celeborn",
                celeborn_lm_host="host",
                celeborn_lm_port=bad_port,
            ),
        ):
            pass


def test_celeborn_app_id_must_not_be_empty():
    """An empty celeborn_app_id must not silently replace the default.

    `(app_id, shuffle_id)` is the shuffle's identity on the cluster, so an empty
    app_id would put unrelated Daft processes in one namespace.
    """
    with (
        pytest.raises(ValueError, match="celeborn_app_id"),
        daft.execution_config_ctx(
            shuffle_algorithm="celeborn",
            celeborn_lm_host="host",
            celeborn_lm_port=9097,
            celeborn_app_id="  ",
        ),
    ):
        pass


def test_celeborn_app_id_defaults_to_a_per_process_value():
    """Leaving celeborn_app_id unset must still yield a usable app_id.

    The default is derived once per process, so a user who only supplies the
    LifecycleManager coordinates still gets a namespace that no other Daft
    process shares.
    """
    with daft.execution_config_ctx(
        shuffle_algorithm="celeborn",
        celeborn_lm_host="host",
        celeborn_lm_port=9097,
    ):
        app_id = daft.context.get_context().daft_execution_config.celeborn_app_id
        assert app_id
        assert app_id.strip() == app_id


def test_celeborn_properties_round_trip():
    """`celeborn.*` tunables travel as opaque properties, not typed options.

    Compression codec, push/fetch timeouts, inflight backpressure and every
    other Celeborn client option share this single channel: Daft forwards the
    pairs verbatim to the native client rather than mirroring each option as its
    own config field. So the assertion here is that the list round-trips
    unchanged, and that Daft does *not* validate the keys or values (an unknown
    key is the Celeborn client's business to reject, not Daft's).
    """
    properties = [
        ("celeborn.client.shuffle.compression.codec", "zstd"),
        ("celeborn.client.push.timeout", "12s"),
        ("celeborn.client.fetch.timeout", "67s"),
        ("celeborn.not.a.real.option", "passed-through-anyway"),
    ]
    with daft.execution_config_ctx(
        shuffle_algorithm="celeborn",
        celeborn_lm_host="host",
        celeborn_lm_port=9097,
        celeborn_properties=properties,
    ):
        cfg = daft.context.get_context().daft_execution_config
        assert cfg.celeborn_properties == properties


def test_invalid_shuffle_algorithm_is_rejected():
    """Sanity: a typo in shuffle_algorithm must still fail loudly.

    This guards against the Celeborn whitelist update accidentally widening the
    accepted set to include arbitrary strings.
    """
    with pytest.raises(ValueError, match="shuffle_algorithm"), daft.execution_config_ctx(shuffle_algorithm="celebornn"):
        pass


@pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="shuffle backend selection only happens in distributed plan translation",
)
@pytest.mark.parametrize(
    "celeborn_options",
    [
        # Nothing but the algorithm: no Celeborn config at all.
        {},
        # A partial config. This one is the interesting case: supplying any
        # `celeborn_*` option materializes the whole config struct, so a check
        # for its mere presence would wave this through with the
        # LifecycleManager coordinates still unset.
        {"celeborn_app_id": "some-app"},
        {"celeborn_lm_host": "host"},
    ],
    ids=["no_celeborn_config", "app_id_only", "host_only"],
)
def test_celeborn_shuffle_does_not_silently_degrade(celeborn_options):
    """A Celeborn run Daft cannot honour must fail, not fall back to Ray.

    `select_backend` maps every unrecognised algorithm onto the Ray backend, so
    without validation at translation entry a build lacking the `celeborn`
    feature — or a config missing the LifecycleManager coordinates — would
    silently run an ordinary Ray shuffle and report success. Either way the
    message must name `celeborn` so the user knows which knob was ignored.
    """
    with daft.execution_config_ctx(shuffle_algorithm="celeborn", **celeborn_options):
        df = daft.from_pydict({"ints": [1, 2, 3, 1]}).repartition(4, "ints")
        with pytest.raises(Exception, match="celeborn"):
            df.collect()
