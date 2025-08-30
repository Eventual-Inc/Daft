"""KV Store configuration and utilities for Daft."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from daft.io import IOConfig

__all__ = ["KVConfig", "LanceConfig"]


@dataclass
class LanceConfig:
    """Configuration for Lance KV Store backend.
    
    Args:
        uri: URI path to the Lance dataset
        io_config: IO configuration for accessing the dataset
        columns: List of column names to retrieve
        batch_size: Batch size for processing optimization
        max_connections: Maximum number of connections
    """
    uri: str
    io_config: Optional[IOConfig] = None
    columns: Optional[list[str]] = None
    batch_size: int = 1000
    max_connections: int = 32

    def with_io_config(self, io_config: IOConfig) -> LanceConfig:
        """Set IO configuration."""
        self.io_config = io_config
        return self

    def with_columns(self, columns: list[str]) -> LanceConfig:
        """Set columns to retrieve."""
        self.columns = columns
        return self

    def with_batch_size(self, batch_size: int) -> LanceConfig:
        """Set batch size."""
        self.batch_size = batch_size
        return self

    def with_max_connections(self, max_connections: int) -> LanceConfig:
        """Set maximum connections."""
        self.max_connections = max_connections
        return self


@dataclass
class KVConfig:
    """Unified configuration for KV Store operations.
    
    This class provides a unified interface for configuring different
    KV Store backends. Currently supports Lance backend.
    
    Args:
        lance: Lance backend configuration
    
    Examples:
        >>> from daft.kv import KVConfig, LanceConfig
        >>> from daft.io import IOConfig
        >>> 
        >>> # Create Lance configuration
        >>> lance_config = LanceConfig(
        ...     uri="s3://my-bucket/my-dataset.lance",
        ...     io_config=IOConfig.s3(region="us-west-2"),
        ...     columns=["image", "metadata"],
        ...     batch_size=500
        ... )
        >>> 
        >>> # Create KV configuration
        >>> kv_config = KVConfig(lance=lance_config)
        >>> 
        >>> # Use in expression
        >>> df = df.with_column("data", df["row_id"].kv.lance.get(kv_config=kv_config))
    """
    lance: Optional[LanceConfig] = None

    @classmethod
    def lance_config(
        cls,
        uri: str,
        io_config: Optional[IOConfig] = None,
        columns: Optional[list[str]] = None,
        batch_size: int = 1000,
        max_connections: int = 32,
    ) -> KVConfig:
        """Create a KVConfig with Lance backend configuration.
        
        Args:
            uri: URI path to the Lance dataset
            io_config: IO configuration for accessing the dataset
            columns: List of column names to retrieve
            batch_size: Batch size for processing optimization
            max_connections: Maximum number of connections
            
        Returns:
            KVConfig: Configured KV config with Lance backend
        """
        lance_config = LanceConfig(
            uri=uri,
            io_config=io_config,
            columns=columns,
            batch_size=batch_size,
            max_connections=max_connections,
        )
        return cls(lance=lance_config)

    def get_active_backend(self) -> str:
        """Get the name of the active backend.
        
        Returns:
            str: Name of the active backend
            
        Raises:
            ValueError: If no backend is configured
        """
        if self.lance is not None:
            return "lance"
        else:
            raise ValueError("No active KV backend configured")

    def is_lance_backend(self) -> bool:
        """Check if Lance backend is configured."""
        return self.lance is not None