#!/bin/bash
set -e

# Add this configuration for accessing MinIO service (only if not already present)
FILESET_CONF="/root/gravitino/catalogs/fileset/conf/fileset.conf"
if ! grep -q "gravitino.bypass.fs.s3a.path.style.access=true" "$FILESET_CONF" 2>/dev/null; then
    echo "gravitino.bypass.fs.s3a.path.style.access=true" >> "$FILESET_CONF"
fi

# Start Gravitino server using the original entrypoint
exec /bin/bash /root/gravitino/bin/start-gravitino.sh
