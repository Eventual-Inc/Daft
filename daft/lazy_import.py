# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Borrowed and modified from [`skypilot`](https://github.com/skypilot-org/skypilot/blob/master/sky/adaptors/common.py).

import importlib
from typing import Any


class LazyImport:
    """Manages Optional Dependency imports.

    There are certain large imports (e.g. Ray, daft.unity_catalog.UnityCatalogTable, etc.) that
    do not need to be top-level imports. For example, Ray should only be imported when the ray
    runner is used, or specific ray data extension types are needed. We can lazily import these
    modules as needed.
    """

    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module = None

    def module_available(self):
        return self._load_module() is not None

    def _load_module(self):
        if self._module is None:
            try:
                self._module = importlib.import_module(self._module_name)
            except ImportError:
                pass
        return self._module

    def __getattr__(self, name: str) -> Any:
        # Given a lazy module and an attribute to get, we have the following possibilities:
        #   1. The attribute is the lazy object's attribute.
        #   2. The attribute is an attribute of the module.
        #   3. The module does not exist.
        #   4. The attribute is a submodule.
        #   5. The attribute does not exist.
        try:
            if name in self.__dict__:
                return self.__dict__[name]
            return getattr(self._load_module(), name)
        except AttributeError as e:
            if self._module is None:
                raise e
            # Dynamically create a new LazyImport instance for the submodule.
            submodule_name = f"{self._module_name}.{name}"
            lazy_submodule = LazyImport(submodule_name)
            if lazy_submodule.module_available():
                setattr(self, name, lazy_submodule)
                return lazy_submodule
            raise e
