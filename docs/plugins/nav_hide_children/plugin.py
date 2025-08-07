from mkdocs.config import config_options
from mkdocs.plugins import BasePlugin
from mkdocs.structure import StructureItem
from mkdocs.structure.nav import Navigation


class NavHideChildrenConfig(config_options.Config):
    hide_children = config_options.Type(list, default=[])


class NavHideChildrenPlugin(BasePlugin[NavHideChildrenConfig]):
    """MkDocs plugin to conditionally hide navigation children based on configured paths."""

    def on_nav(self, nav: Navigation, /, *, config, files) -> Navigation:
        paths_to_hide = self.config.hide_children.copy()
        for item in nav:
            set_hide_children(paths_to_hide, item)

        if len(paths_to_hide) > 0:
            path_list = "\n".join(f" - {path}" for path in paths_to_hide)
            raise ValueError(f"Paths in nav-hide-children.hide_children do not exist:\n{path_list}")

        return nav


def set_hide_children(paths_to_hide: list[list[str]], item: StructureItem, curr_path: list[str] | None = None):
    if curr_path is None:
        curr_path = []
    if item.title is None:
        return

    next_path = curr_path + [item.title]
    if next_path in paths_to_hide:
        item.hide_children = True
        paths_to_hide.remove(next_path)

    if item.children is not None:
        for child in item.children:
            set_hide_children(paths_to_hide, child, next_path)
