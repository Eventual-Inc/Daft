# Daft MkDocs Plugins

Custom MkDocs plugins for the Daft documentation site.

## nav-hide-children

A plugin that allows you to conditionally hide navigation children based on configured paths.

### Configuration

Add to your `mkdocs.yml`:

```yaml
plugins:
  - nav-hide-children:
      hide_children:
        - ["Python API", "Functions"]  # Hide children of Python API > Functions
        - ["SQL Reference"]            # Hide children of SQL Reference
```

### Usage

The plugin automatically injects an attribute `hide_children` into [navigation objects](https://www.mkdocs.org/dev-guide/themes/#navigation-objects) based on the config paths, which can be used in Jinja2 templates to conditionally hide navigation elements.
