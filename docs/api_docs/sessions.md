# Sessions

Sessions enable you to attach catalogs, tables, and create temporary objects which are accessible through both the Python and SQL APIs. Sessions hold configuration state such as current_catalog and current_namespace which are used in name resolution and can simplify your workflows. Learn more about [Sessions](../sessions.md) in Daft User Guide.

::: daft.session.Session
    options:
        filters: ["!^_"]

<!-- add more pages to filters to include them, see dataframe for example -->

<!-- fix: do we need class session? -->
