# Sessions

Sessions enable you to attach objects such as catalogs, providers, models, functions, and tables which are reference-able in DataFrame operations. The session also enables creating temporary objects which are accessible through both the Python and SQL APIs. Sessions hold configuration state such as current_catalog and current_namespace which are used in name resolution and can simplify your workflows. Learn more about [Sessions](../sessions.md) in the Daft User Guide.

::: daft.session.session
    options:
        heading_level: 3

::: daft.session.current_session
    options:
        heading_level: 3

::: daft.session.Session
    options:
        filters: ["!^_"]

## Active Session

These methods resolve to the active session for the given scope.

::: daft.session.attach
    options:
        heading_level: 3

::: daft.session.attach_catalog
    options:
        heading_level: 3

::: daft.session.attach_function
    options:
        heading_level: 3

::: daft.session.attach_provider
    options:
        heading_level: 3

::: daft.session.attach_table
    options:
        heading_level: 3

::: daft.session.detach_catalog
    options:
        heading_level: 3

::: daft.session.detach_function
    options:
        heading_level: 3

::: daft.session.detach_provider
    options:
        heading_level: 3

::: daft.session.detach_table
    options:
        heading_level: 3

::: daft.session.create_namespace
    options:
        heading_level: 3

::: daft.session.create_namespace_if_not_exists
    options:
        heading_level: 3

::: daft.session.create_table
    options:
        heading_level: 3

::: daft.session.create_table_if_not_exists
    options:
        heading_level: 3

::: daft.session.create_temp_table
    options:
        heading_level: 3

::: daft.session.drop_namespace
    options:
        heading_level: 3

::: daft.session.drop_table
    options:
        heading_level: 3

::: daft.session.current_catalog
    options:
        heading_level: 3

::: daft.session.current_namespace
    options:
        heading_level: 3

::: daft.session.current_model
    options:
        heading_level: 3

::: daft.session.current_provider
    options:
        heading_level: 3

::: daft.session.get_catalog
    options:
        heading_level: 3

::: daft.session.get_provider
    options:
        heading_level: 3

::: daft.session.get_table
    options:
        heading_level: 3

::: daft.session.has_catalog
    options:
        heading_level: 3

::: daft.session.has_provider
    options:
        heading_level: 3

::: daft.session.has_namespace
    options:
        heading_level: 3

::: daft.session.has_table
    options:
        heading_level: 3

::: daft.session.list_catalogs
    options:
        heading_level: 3

::: daft.session.list_namespaces
    options:
        heading_level: 3

::: daft.session.list_tables
    options:
        heading_level: 3

::: daft.session.read_table
    options:
        heading_level: 3

::: daft.session.write_table
    options:
        heading_level: 3

::: daft.session.set_catalog
    options:
        heading_level: 3

::: daft.session.set_namespace
    options:
        heading_level: 3

::: daft.session.set_provider
    options:
        heading_level: 3

::: daft.session.set_model
    options:
        heading_level: 3
