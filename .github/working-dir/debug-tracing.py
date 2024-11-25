import daft

df = daft.from_pydict({"foo": [1, 2, 3]})
df.show()

print("Job is running")
print("Context:", daft.context.get_context())
print("Ray tracing enabled:", daft.context.get_context().daft_execution_config)
