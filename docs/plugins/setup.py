"""Setup configuration for Daft MkDocs plugins."""

from setuptools import find_packages, setup

setup(
    name="daft-mkdocs-plugins",
    version="0.1.0",
    description="Custom MkDocs plugins for Daft documentation",
    packages=find_packages(),
    install_requires=[
        "mkdocs>=1.4",
    ],
    entry_points={
        "mkdocs.plugins": [
            "nav-hide-children = nav_hide_children.plugin:NavHideChildrenPlugin",
        ]
    },
    python_requires=">=3.8",
)
