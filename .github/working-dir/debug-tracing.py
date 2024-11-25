import os

import daft

df = daft.from_pydict({"foo": [1, 2, 3]})
df.show()

print("Job is running import")
print("import Job ID:", os.getenv("RAY_JOB_ID"))
print("import Runner envvar:", os.getenv("DAFT_RUNNER"))
print("import Tracing envvar:", os.getenv("DAFT_ENABLE_RAY_TRACING"))
print("import Context:", daft.context.get_context())
print("import Ray tracing enabled:", daft.context.get_context().daft_execution_config.enable_ray_tracing)

if __name__ == "__main__":
    print("Job is running __main__")
    print("__main__ Job ID:", os.getenv("RAY_JOB_ID"))
    print("__main__ Runner envvar:", os.getenv("DAFT_RUNNER"))
    print("__main__ Tracing envvar:", os.getenv("DAFT_ENABLE_RAY_TRACING"))
    print("__main__ Context:", daft.context.get_context())
    print("__main__ Ray tracing enabled:", daft.context.get_context().daft_execution_config.enable_ray_tracing)
