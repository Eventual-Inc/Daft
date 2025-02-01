import daft


def test_session():
    session = daft.current_session()
    # session.set_default_catalog("default")
    # session.set_default_namespace("default")
    print(repr(session))
