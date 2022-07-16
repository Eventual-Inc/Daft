import io
from typing import Any, Callable

import cloudpickle

HandlerFunction = Callable[[Any], Any]


def dump_function(func: HandlerFunction, f: io.BufferedWriter) -> None:
    # TODO(jay): Implement pickle modules by value vs by reference
    f.write(cloudpickle.dumps(func))
