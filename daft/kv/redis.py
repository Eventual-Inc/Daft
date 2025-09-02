"""Redis-based KV Store implementation for Daft."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.kv import KVConfig

from daft.kv import KVConfig, KVStore


class RedisKVStore(KVStore):
    """Redis-based KV Store implementation.

    This class provides a Redis-backed key-value store that integrates
    with Daft's session management system. Redis is an in-memory data
    structure store used as a database, cache, and message broker.

    Examples:
        >>> from daft.kv.redis import RedisKVStore
        >>> # Create a Redis KV store
        >>> redis_kv = RedisKVStore(name="my_redis_store", host="localhost", port=6379, db=0)
        >>> # Use with session
        >>> import daft
        >>> daft.attach(redis_kv, alias="cache")
        >>> daft.set_kv("cache")
    """

    def __init__(
        self,
        name: str | None = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: str | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_keepalive: bool = False,
        socket_keepalive_options: dict[str, Any] | None = None,
        connection_pool: Any | None = None,
        unix_socket_path: str | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = False,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: str | None = None,
        ssl_check_hostname: bool = False,
        max_connections: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Redis KV Store.

        Args:
            name: Optional name for the KV store instance
            host: Redis server hostname
            port: Redis server port
            db: Redis database number
            password: Password for Redis authentication
            socket_timeout: Socket timeout in seconds
            socket_connect_timeout: Socket connection timeout in seconds
            socket_keepalive: Enable TCP keepalive
            socket_keepalive_options: TCP keepalive options
            connection_pool: Custom connection pool
            unix_socket_path: Unix socket path (alternative to host/port)
            encoding: String encoding
            encoding_errors: Error handling for encoding
            decode_responses: Automatically decode responses
            retry_on_timeout: Retry operations on timeout
            ssl: Enable SSL/TLS
            ssl_keyfile: SSL private key file
            ssl_certfile: SSL certificate file
            ssl_cert_reqs: SSL certificate requirements
            ssl_ca_certs: SSL CA certificates file
            ssl_check_hostname: Check SSL hostname
            max_connections: Maximum number of connections
            **kwargs: Additional Redis-specific options
        """
        self._name = name or "redis_kv_store"
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._socket_timeout = socket_timeout
        self._socket_connect_timeout = socket_connect_timeout
        self._socket_keepalive = socket_keepalive
        self._socket_keepalive_options = socket_keepalive_options or {}
        self._connection_pool = connection_pool
        self._unix_socket_path = unix_socket_path
        self._encoding = encoding
        self._encoding_errors = encoding_errors
        self._decode_responses = decode_responses
        self._retry_on_timeout = retry_on_timeout
        self._ssl = ssl
        self._ssl_keyfile = ssl_keyfile
        self._ssl_certfile = ssl_certfile
        self._ssl_cert_reqs = ssl_cert_reqs
        self._ssl_ca_certs = ssl_ca_certs
        self._ssl_check_hostname = ssl_check_hostname
        self._max_connections = max_connections
        self._kwargs = kwargs

        # For Redis store, we don't have a specific backend config in KVConfig yet
        # We'll create a minimal KVConfig for compatibility
        # TODO: Add RedisConfig to KVConfig when needed
        self._kv_config = KVConfig()

    @property
    def name(self) -> str:
        """Returns the KV store's name."""
        return self._name

    @property
    def backend_type(self) -> str:
        """Returns the backend type."""
        return "redis"

    def get_config(self) -> KVConfig:
        """Returns the KV store's configuration."""
        return self._kv_config

    @property
    def host(self) -> str:
        """Returns the Redis server hostname."""
        return self._host

    @property
    def port(self) -> int:
        """Returns the Redis server port."""
        return self._port

    @property
    def db(self) -> int:
        """Returns the Redis database number."""
        return self._db

    @property
    def connection_info(self) -> dict[str, Any]:
        """Returns Redis connection information."""
        info = {
            "host": self._host,
            "port": self._port,
            "db": self._db,
            "ssl": self._ssl,
        }
        if self._unix_socket_path:
            info["unix_socket_path"] = self._unix_socket_path
        return info

    def with_db(self, db: int) -> RedisKVStore:
        """Create a new Redis KV store with different database number.

        Args:
            db: Redis database number

        Returns:
            RedisKVStore: New Redis KV store instance with updated database
        """
        return RedisKVStore(
            name=self._name,
            host=self._host,
            port=self._port,
            db=db,
            password=self._password,
            socket_timeout=self._socket_timeout,
            socket_connect_timeout=self._socket_connect_timeout,
            socket_keepalive=self._socket_keepalive,
            socket_keepalive_options=self._socket_keepalive_options,
            connection_pool=self._connection_pool,
            unix_socket_path=self._unix_socket_path,
            encoding=self._encoding,
            encoding_errors=self._encoding_errors,
            decode_responses=self._decode_responses,
            retry_on_timeout=self._retry_on_timeout,
            ssl=self._ssl,
            ssl_keyfile=self._ssl_keyfile,
            ssl_certfile=self._ssl_certfile,
            ssl_cert_reqs=self._ssl_cert_reqs,
            ssl_ca_certs=self._ssl_ca_certs,
            ssl_check_hostname=self._ssl_check_hostname,
            max_connections=self._max_connections,
            **self._kwargs,
        )

    def with_host_port(self, host: str, port: int) -> RedisKVStore:
        """Create a new Redis KV store with different host and port.

        Args:
            host: Redis server hostname
            port: Redis server port

        Returns:
            RedisKVStore: New Redis KV store instance with updated host/port
        """
        return RedisKVStore(
            name=self._name,
            host=host,
            port=port,
            db=self._db,
            password=self._password,
            socket_timeout=self._socket_timeout,
            socket_connect_timeout=self._socket_connect_timeout,
            socket_keepalive=self._socket_keepalive,
            socket_keepalive_options=self._socket_keepalive_options,
            connection_pool=self._connection_pool,
            unix_socket_path=self._unix_socket_path,
            encoding=self._encoding,
            encoding_errors=self._encoding_errors,
            decode_responses=self._decode_responses,
            retry_on_timeout=self._retry_on_timeout,
            ssl=self._ssl,
            ssl_keyfile=self._ssl_keyfile,
            ssl_certfile=self._ssl_certfile,
            ssl_cert_reqs=self._ssl_cert_reqs,
            ssl_ca_certs=self._ssl_ca_certs,
            ssl_check_hostname=self._ssl_check_hostname,
            max_connections=self._max_connections,
            **self._kwargs,
        )

    def with_ssl(self, ssl: bool = True, **ssl_options: Any) -> RedisKVStore:
        """Create a new Redis KV store with SSL/TLS configuration.

        Args:
            ssl: Enable SSL/TLS
            **ssl_options: SSL configuration options

        Returns:
            RedisKVStore: New Redis KV store instance with SSL configuration
        """
        ssl_keyfile = ssl_options.get("ssl_keyfile", self._ssl_keyfile)
        ssl_certfile = ssl_options.get("ssl_certfile", self._ssl_certfile)
        ssl_cert_reqs = ssl_options.get("ssl_cert_reqs", self._ssl_cert_reqs)
        ssl_ca_certs = ssl_options.get("ssl_ca_certs", self._ssl_ca_certs)
        ssl_check_hostname = ssl_options.get("ssl_check_hostname", self._ssl_check_hostname)

        return RedisKVStore(
            name=self._name,
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._password,
            socket_timeout=self._socket_timeout,
            socket_connect_timeout=self._socket_connect_timeout,
            socket_keepalive=self._socket_keepalive,
            socket_keepalive_options=self._socket_keepalive_options,
            connection_pool=self._connection_pool,
            unix_socket_path=self._unix_socket_path,
            encoding=self._encoding,
            encoding_errors=self._encoding_errors,
            decode_responses=self._decode_responses,
            retry_on_timeout=self._retry_on_timeout,
            ssl=ssl,
            ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile,
            ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs,
            ssl_check_hostname=ssl_check_hostname,
            max_connections=self._max_connections,
            **self._kwargs,
        )

    def __repr__(self) -> str:
        """String representation of the Redis KV store."""
        if self._unix_socket_path:
            connection = f"unix_socket_path='{self._unix_socket_path}'"
        else:
            connection = f"host='{self._host}', port={self._port}"
        return f"RedisKVStore(name='{self._name}', {connection}, " f"db={self._db}, ssl={self._ssl})"

    def __str__(self) -> str:
        """Human-readable string representation."""
        if self._unix_socket_path:
            location = f"unix socket {self._unix_socket_path}"
        else:
            location = f"{self._host}:{self._port}"
        return f"Redis KV Store '{self._name}' at {location} (db={self._db})"
