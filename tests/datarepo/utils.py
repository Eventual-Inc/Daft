from icebridge.client import IceBridgeClient, IcebergCatalog


def create_test_catalog(path: str) -> IcebergCatalog:
    client = IceBridgeClient()
    hadoop_catalog = IcebergCatalog.from_hadoop_catalog(client, f"file://{path}")
    return hadoop_catalog
