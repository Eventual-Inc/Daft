import os
import daft
from datetime import datetime

N = 10
# LIM = 0x110000
LIM = 1000
df = daft.from_pydict({"a": list(range(1, N)), "b": [chr(x % LIM) for x in range(1, N)]})


start = datetime.now()
r = df.write_sql("testing_a", os.environ["SUPABASE_CONNECTION_STR"], mode='overwrite')
stop = datetime.now()

print(f"done! took: {stop - start} -- results:\n{r}")
