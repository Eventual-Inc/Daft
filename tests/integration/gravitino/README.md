# Gravitino Integration Tests

These tests exercise Daft's Gravitino connector end-to-end. To run them:

1. Start the local Gravitino service:

   ```bash
   cd tests/integration/gravitino/docker-compose
   docker compose up -d
   ```

2. Export the connection settings (if you use the compose file above the defaults already match):

   ```bash
   export GRAVITINO_ENDPOINT=${GRAVITINO_ENDPOINT:-http://127.0.0.1:8090}
   export GRAVITINO_METALAKE=${GRAVITINO_METALAKE:-metalake_demo}
   ```

3. Run the tests:

   ```bash
   DAFT_RUNNER=native pytest tests/integration/gravitino -v -m integration
   ```

Optionally, configure `GRAVITINO_TEST_FILE` and `GRAVITINO_TEST_DIR` to point at a fileset that exists in
your Gravitino deployment so that the gvfs:// IO tests can read concrete data.

The catalog integration test (`test_gravitino_integration.py`) provisions a temporary catalog/schema/fileset via
the Gravitino REST API and writes a Parquet file into a temporary local directory. The test cleans up both the
server-side metadata and the local directory when it finishes.
