import daft

df = daft.from_pydict({"A": [3, 3, 1, 1], "B": ["abcdef", "barstufdk", "uiop[hjk]", ""], "C": ["aaa", "a", "aa", "aa"]})

# df = df.with_column("B2", df["B"].str.substr(df["A"] + 1, df["B"].str.length()))
df = df.with_column("Substr", df["B"].str.substr(0, 4))

df.show()
