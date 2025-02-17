import daft

###
# SESSION SETUP
###


def test_current_session_exists():
    assert daft.current_session() is not None
