#!/bin/bash
set -e

# Add this configuration for accessing MinIO service (only if not already present)
FILESET_CONF="/root/gravitino/catalogs/fileset/conf/fileset.conf"
if ! grep -q "gravitino.bypass.fs.s3a.path.style.access=true" "$FILESET_CONF" 2>/dev/null; then
    echo "gravitino.bypass.fs.s3a.path.style.access=true" >> "$FILESET_CONF"
fi

# Run the original config rewrite script first
cd /root/gravitino
python bin/rewrite_gravitino_server_config.py

# Configure Iceberg REST service with local filesystem AFTER the rewrite
GRAVITINO_CONF="/root/gravitino/conf/gravitino.conf"

# Create warehouse directory
mkdir -p /tmp/gravitino-warehouse

# Add Iceberg REST configuration if not already present
if ! grep -q "gravitino.iceberg-rest.httpPort" "$GRAVITINO_CONF" 2>/dev/null; then
    echo "" >> "$GRAVITINO_CONF"
    echo "# Iceberg REST service configuration" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.classpath = iceberg-rest-server/libs,iceberg-rest-server/conf" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.host = 0.0.0.0" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.httpPort = 9001" >> "$GRAVITINO_CONF"

    # Use JDBC backend with MySQL
    echo "gravitino.iceberg-rest.catalog-backend = jdbc" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.uri = jdbc:mysql://daft-mysql:3306/iceberg_catalog" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.jdbc-driver = com.mysql.cj.jdbc.Driver" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.jdbc-user = root" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.jdbc-password = root" >> "$GRAVITINO_CONF"
    echo "gravitino.iceberg-rest.jdbc-initialize = true" >> "$GRAVITINO_CONF"

    # Use local filesystem warehouse (shared with host via volume mount)
    echo "gravitino.iceberg-rest.warehouse = file:///tmp/gravitino-warehouse/" >> "$GRAVITINO_CONF"
fi

# Start Gravitino server directly (skip the rewrite script since we already ran it)
export JAVA_OPTS="${JAVA_OPTS} -XX:-UseContainerSupport"
exec ./bin/gravitino.sh start
