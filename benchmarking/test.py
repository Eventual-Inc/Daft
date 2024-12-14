import daft

df = daft.from_pydict({"nums": [1, 2, 3]})

df.show()
