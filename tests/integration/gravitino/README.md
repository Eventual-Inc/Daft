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

## Test Files

- `test_gravitino_fileset.py` - Tests filesets with local file:// storage
- `test_gravitino_fileset_s3.py` - Tests filesets with S3 storage (using MinIO)
- `test_gravitino_table.py` - Tests table catalog operations

## S3 Storage Tests

The S3 fileset tests (`test_gravitino_fileset_s3.py`) test Gravitino filesets backed by S3 storage.

### Current Status


The docker-compose setup includes:
1. **MinIO**: S3-compatible storage on port 9000
2. **Update fileset.conf**: To run with MinIO with s3 need to set a property in `/root/gravitino/catalogs/fileset/conf/fileset.conf`
3. **Test Infrastructure**: Complete test suite for S3 filesets

### Running S3 Tests with External Gravitino

If you have access to a Gravitino deployment with S3 filesets already configured, you can test against it:

```bash
# Point to an existing S3-backed fileset
export GRAVITINO_TEST_DIR="gvfs://fileset/<catalog>/<schema>/<fileset>/"
export GRAVITINO_TEST_FILE="gvfs://fileset/<catalog>/<schema>/<fileset>/file.parquet"
export GRAVITINO_ENDPOINT="http://your-gravitino-server:8090"

# Run specific tests
DAFT_RUNNER=native pytest tests/integration/gravitino/test_gravitino_fileset_s3.py::test_read_existing_s3_fileset -v -m integration
```

### Test Coverage

The S3 test suite includes:
- Basic read operations from S3-backed filesets
- Glob patterns and file listing
- Partitioned data handling
- Error handling for missing files and empty directories

## Optional Configuration

Optionally, configure `GRAVITINO_TEST_FILE` and `GRAVITINO_TEST_DIR` to point at a fileset that exists in
your Gravitino deployment so that the gvfs:// IO tests can read concrete data.

The catalog integration tests provision temporary catalog/schema/fileset via the Gravitino REST API and write
test data to either local storage or MinIO. The tests clean up both the server-side metadata and storage when finished.
