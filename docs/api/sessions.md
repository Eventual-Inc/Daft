# Sessions

Sessions enable you to attach objects such as catalogs, providers, models, functions, and tables which are reference-able in DataFrame operations. The session also enables creating temporary objects which are accessible through both the Python and SQL APIs. Sessions hold configuration state such as current_catalog and current_namespace which are used in name resolution and can simplify your workflows. Learn more about [Sessions](../configuration/sessions-usage.md) in the Daft User Guide.

::: daft.session.session
    options:
        heading_level: 3

::: daft.session.current_session
    options:
        heading_level: 3

::: daft.session.Session
    options:
        filters: ["!^_"]
